/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpc

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal/balancer/gracefulswitch"
	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/channelz"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

// ccBalancerWrapper 是grpc.ClientConn 和 Balancer之间的桥梁

// ccBalancerWrapper sits between the ClientConn and the Balancer.
//
// ccBalancerWrapper implements methods corresponding to the ones on the
// balancer.Balancer interface. The ClientConn is free to call these methods
// concurrently and the ccBalancerWrapper ensures that calls from the ClientConn
// to the Balancer happen synchronously and in order.
//
// ccBalancerWrapper also implements the balancer.ClientConn interface and is
// passed to the Balancer implementations. It invokes unexported methods on the
// ClientConn to handle these calls from the Balancer.
//
// It uses the gracefulswitch.Balancer internally to ensure that balancer
// switches happen in a graceful manner.
type ccBalancerWrapper struct {
	cc *ClientConn

	// Since these fields are accessed only from handleXxx() methods which are
	// synchronized by the watcher goroutine, we do not need a mutex to protect
	// these fields.
	balancer        *gracefulswitch.Balancer
	curBalancerName string

	updateCh *buffer.Unbounded // Updates written on this channel are processed by watcher().
	resultCh *buffer.Unbounded // Results of calls to UpdateClientConnState() are pushed here.
	closed   *grpcsync.Event   // Indicates if close has been called.
	done     *grpcsync.Event   // Indicates if close has completed its work.
}

// newCCBalancerWrapper creates a new balancer wrapper. The underlying balancer
// is not created until the switchTo() method is invoked.
func newCCBalancerWrapper(cc *ClientConn, bopts balancer.BuildOptions) *ccBalancerWrapper {
	ccb := &ccBalancerWrapper{
		cc:       cc,
		updateCh: buffer.NewUnbounded(),
		resultCh: buffer.NewUnbounded(),
		closed:   grpcsync.NewEvent(),
		done:     grpcsync.NewEvent(),
	}
	go ccb.watcher()
	ccb.balancer = gracefulswitch.NewBalancer(ccb, bopts)
	return ccb
}

// The following xxxUpdate structs wrap the arguments received as part of the
// corresponding update. The watcher goroutine uses the 'type' of the update to
// invoke the appropriate handler routine to handle the update.

type ccStateUpdate struct {
	ccs *balancer.ClientConnState
}

type scStateUpdate struct {
	sc    balancer.SubConn
	state connectivity.State
	err   error
}

type exitIdleUpdate struct{}

type resolverErrorUpdate struct {
	err error
}

type switchToUpdate struct {
	// name 负载均衡策略的名字
	name string
}

type subConnUpdate struct {
	acbw *acBalancerWrapper
}

// watcher是一个长时间运行的goroutine，他从channel中读取更新事件，然后调用底层的balancer的对应方法。他确保这些方法以同步的方式被调用,
// 他也确保这些方法按接收update事件的顺序被调用。
// 调用底层的balancer的对应方法，调用链是这样: *ccBalancerWrapper包含了*gracefulswitch.Balancer，*gracefulswitch.Balancer
// 可以获取到当前使用的Balancer（Balancer接口的实现），*balancerWrapper, 然后调用*balancerWrapper的方法

// watcher is a long-running goroutine which reads updates from a channel and
// invokes corresponding methods on the underlying balancer. It ensures that
// these methods are invoked in a synchronous fashion. It also ensures that
// these methods are invoked in the order in which the updates were received.
func (ccb *ccBalancerWrapper) watcher() {
	for {
		select {
		case u := <-ccb.updateCh.Get():
			ccb.updateCh.Load()
			if ccb.closed.HasFired() {
				break
			}
			switch update := u.(type) {
			case *ccStateUpdate:
				fmt.Println("收到ccStateUpdate事件")
				// 3. 调用了最新的(pending)负载均衡器的UpdateClientConnState()方法，比如baseBalancer.UpdateClientConnState()
				ccb.handleClientConnStateChange(update.ccs)
			case *scStateUpdate:
				// 4. 某个连接状态变化事件
				// 触发场景：1.服务列表变化，发现有新的地址，此时需要创建连接，创建连接之前将连接状态置为Connecting
				//         2. 连接创建成功后，会设置连接状态为Ready
				ccb.handleSubConnStateChange(update)
			case *exitIdleUpdate:
				ccb.handleExitIdle()
			case *resolverErrorUpdate:
				ccb.handleResolverError(update.err)
			case *switchToUpdate:
				fmt.Println("收到switchToUpdate事件")
				// 2. 在Resolver触发服务地址变更后，会发送这个事件。
				// 这块逻辑，通过负载均衡名，从全局map中拿到对应的BalancerBuilder，并调用其Build()方法，生成新的Balancer,作为pending。仅仅是根据名字创建balancer
				ccb.handleSwitchTo(update.name)
			case *subConnUpdate:
				// 清除连接
				// 触发场景：1. 在新的一次Resolver服务发现的过程中，某个地址已经被删除。此时需要移除对应的连接
				// 触发场景：2. 某个服务地址连接出现错误，在尝试一定次数的重连后，仍然失败
				ccb.handleRemoveSubConn(update.acbw)
			default:
				logger.Errorf("ccBalancerWrapper.watcher: unknown update %+v, type %T", update, update)
			}
		case <-ccb.closed.Done():
		}

		if ccb.closed.HasFired() {
			ccb.handleClose()
			return
		}
	}
}

// updateClientConnState is invoked by grpc to push a ClientConnState update to
// the underlying balancer.
//
// Unlike other methods invoked by grpc to push updates to the underlying
// balancer, this method cannot simply push the update onto the update channel
// and return. It needs to return the error returned by the underlying balancer
// back to grpc which propagates that to the resolver.
func (ccb *ccBalancerWrapper) updateClientConnState(ccs *balancer.ClientConnState) error {
	ccb.updateCh.Put(&ccStateUpdate{ccs: ccs})

	// 等待结果: 上面发送的事件被处理完成后，会向ccb.resultCh发送一个结果(error类型)，我们再这里等待结果的完成。
	// 所以可以认为这个调用是同步的。

	var res interface{}
	select {
	case res = <-ccb.resultCh.Get():
		ccb.resultCh.Load()
	case <-ccb.closed.Done():
		// Return early if the balancer wrapper is closed while we are waiting for
		// the underlying balancer to process a ClientConnState update.
		return nil
	}
	// If the returned error is nil, attempting to type assert to error leads to
	// panic. So, this needs to handled separately.
	if res == nil {
		return nil
	}
	return res.(error)
}

// handleClientConnStateChange handles a ClientConnState update from the update
// channel and invokes the appropriate method on the underlying balancer.
//
// If the addresses specified in the update contain addresses of type "grpclb"
// and the selected LB policy is not "grpclb", these addresses will be filtered
// out and ccs will be modified with the updated address list.
func (ccb *ccBalancerWrapper) handleClientConnStateChange(ccs *balancer.ClientConnState) {
	// 如果负载均衡器使用的不是 grpclbName， 则把服务发现得到的地址中的resolver.GRPCLB类型的过滤掉
	if ccb.curBalancerName != grpclbName {
		// Filter any grpclb addresses since we don't have the grpclb balancer.
		var addrs []resolver.Address
		for _, addr := range ccs.ResolverState.Addresses {
			if addr.Type == resolver.GRPCLB {
				continue
			}
			addrs = append(addrs, addr)
		}
		ccs.ResolverState.Addresses = addrs
	}

	// 发送结果
	ccb.resultCh.Put(ccb.balancer.UpdateClientConnState(*ccs))
}

// updateSubConnState is invoked by grpc to push a subConn state update to the
// underlying balancer.
func (ccb *ccBalancerWrapper) updateSubConnState(sc balancer.SubConn, s connectivity.State, err error) {
	// When updating addresses for a SubConn, if the address in use is not in
	// the new addresses, the old ac will be tearDown() and a new ac will be
	// created. tearDown() generates a state change with Shutdown state, we
	// don't want the balancer to receive this state change. So before
	// tearDown() on the old ac, ac.acbw (acWrapper) will be set to nil, and
	// this function will be called with (nil, Shutdown). We don't need to call
	// balancer method in this case.
	if sc == nil {
		return
	}
	ccb.updateCh.Put(&scStateUpdate{
		sc:    sc,
		state: s,
		err:   err,
	})
}

// handleSubConnStateChange handles a SubConnState update from the update
// channel and invokes the appropriate method on the underlying balancer.
func (ccb *ccBalancerWrapper) handleSubConnStateChange(update *scStateUpdate) {
	ccb.balancer.UpdateSubConnState(update.sc, balancer.SubConnState{ConnectivityState: update.state, ConnectionError: update.err})
}

func (ccb *ccBalancerWrapper) exitIdle() {
	ccb.updateCh.Put(&exitIdleUpdate{})
}

func (ccb *ccBalancerWrapper) handleExitIdle() {
	if ccb.cc.GetState() != connectivity.Idle {
		return
	}
	ccb.balancer.ExitIdle()
}

func (ccb *ccBalancerWrapper) resolverError(err error) {
	ccb.updateCh.Put(&resolverErrorUpdate{err: err})
}

func (ccb *ccBalancerWrapper) handleResolverError(err error) {
	ccb.balancer.ResolverError(err)
}

// switchTo is invoked by grpc to instruct the balancer wrapper to switch to the
// LB policy identified by name.
//
// ClientConn calls newCCBalancerWrapper() at creation time. Upon receipt of the
// first good update from the name resolver, it determines the LB policy to use
// and invokes the switchTo() method. Upon receipt of every subsequent update
// from the name resolver, it invokes this method.
//
// the ccBalancerWrapper keeps track of the current LB policy name, and skips
// the graceful balancer switching process if the name does not change.
func (ccb *ccBalancerWrapper) switchTo(name string) {
	ccb.updateCh.Put(&switchToUpdate{name: name})
}

// handleSwitchTo handles a balancer switch update from the update channel. It
// calls the SwitchTo() method on the gracefulswitch.Balancer with a
// balancer.Builder corresponding to name. If no balancer.Builder is registered
// for the given name, it uses the default LB policy which is "pick_first".
func (ccb *ccBalancerWrapper) handleSwitchTo(name string) {
	// 这里的处理逻辑，一直被我忽略了。
	// 当负载均衡的名字，没有发生变化时，不会重新生成负载均衡器.

	// TODO: Other languages use case-insensitive balancer registries. We should
	// switch as well. See: https://github.com/grpc/grpc-go/issues/5288.
	if strings.EqualFold(ccb.curBalancerName, name) {
		fmt.Println("负载均衡没变化")
		return
	}

	fmt.Printf("初始化负载均衡器[%s]............\n", name)

	// 通过名字，从全局balancer表中获取balancer.Builder
	// TODO: Ensure that name is a registered LB policy when we get here.
	// We currently only validate the `loadBalancingConfig` field. We need to do
	// the same for the `loadBalancingPolicy` field and reject the service config
	// if the specified policy is not registered.
	builder := balancer.Get(name)
	if builder == nil {
		channelz.Warningf(logger, ccb.cc.channelzID, "Channel switches to new LB policy %q, since the specified LB policy %q was not registered", PickFirstBalancerName, name)
		builder = newPickfirstBuilder()
	} else {
		channelz.Infof(logger, ccb.cc.channelzID, "Channel switches to new LB policy %q", name)
	}

	// 通过balancer.Builder创建的新的balancer
	if err := ccb.balancer.SwitchTo(builder); err != nil {
		channelz.Errorf(logger, ccb.cc.channelzID, "Channel failed to build new LB policy %q: %v", name, err)
		return
	}
	ccb.curBalancerName = builder.Name()
}

// handleRemoveSucConn handles a request from the underlying balancer to remove
// a subConn.
//
// See comments in RemoveSubConn() for more details.
func (ccb *ccBalancerWrapper) handleRemoveSubConn(acbw *acBalancerWrapper) {
	ccb.cc.removeAddrConn(acbw.getAddrConn(), errConnDrain)
}

func (ccb *ccBalancerWrapper) close() {
	ccb.closed.Fire()
	<-ccb.done.Done()
}

func (ccb *ccBalancerWrapper) handleClose() {
	ccb.balancer.Close()
	ccb.done.Fire()
}

func (ccb *ccBalancerWrapper) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	if len(addrs) <= 0 {
		return nil, fmt.Errorf("grpc: cannot create SubConn with empty address list")
	}
	// 这里调用的是grpc.ClientConn.newAddrConn
	ac, err := ccb.cc.newAddrConn(addrs, opts)
	if err != nil {
		channelz.Warningf(logger, ccb.cc.channelzID, "acBalancerWrapper: NewSubConn: failed to newAddrConn: %v", err)
		return nil, err
	}
	acbw := &acBalancerWrapper{ac: ac, producers: make(map[balancer.ProducerBuilder]*refCountedProducer)}
	acbw.ac.mu.Lock()
	ac.acbw = acbw
	acbw.ac.mu.Unlock()
	return acbw, nil
}

func (ccb *ccBalancerWrapper) RemoveSubConn(sc balancer.SubConn) {
	debug.PrintStack()
	// Before we switched the ccBalancerWrapper to use gracefulswitch.Balancer, it
	// was required to handle the RemoveSubConn() method asynchronously by pushing
	// the update onto the update channel. This was done to avoid a deadlock as
	// switchBalancer() was holding cc.mu when calling Close() on the old
	// balancer, which would in turn call RemoveSubConn().
	//
	// With the use of gracefulswitch.Balancer in ccBalancerWrapper, handling this
	// asynchronously is probably not required anymore since the switchTo() method
	// handles the balancer switch by pushing the update onto the channel.
	// TODO(easwars): Handle this inline.
	acbw, ok := sc.(*acBalancerWrapper)
	if !ok {
		return
	}
	ccb.updateCh.Put(&subConnUpdate{acbw: acbw})
}

func (ccb *ccBalancerWrapper) UpdateAddresses(sc balancer.SubConn, addrs []resolver.Address) {
	acbw, ok := sc.(*acBalancerWrapper)
	if !ok {
		return
	}
	acbw.UpdateAddresses(addrs)
}

func (ccb *ccBalancerWrapper) UpdateState(s balancer.State) {
	// Update picker before updating state.  Even though the ordering here does
	// not matter, it can lead to multiple calls of Pick in the common start-up
	// case where we wait for ready and then perform an RPC.  If the picker is
	// updated later, we could call the "connecting" picker when the state is
	// updated, and then call the "ready" picker after the picker gets updated.
	ccb.cc.blockingpicker.updatePicker(s.Picker)

	// 更新grpc.ClientConn的状态
	ccb.cc.csMgr.updateState(s.ConnectivityState)
}

func (ccb *ccBalancerWrapper) ResolveNow(o resolver.ResolveNowOptions) {
	ccb.cc.resolveNow(o)
}

func (ccb *ccBalancerWrapper) Target() string {
	return ccb.cc.target
}

// acBalancerWrapper is a wrapper on top of ac for balancers.
// It implements balancer.SubConn interface.
type acBalancerWrapper struct {
	mu        sync.Mutex
	ac        *addrConn
	producers map[balancer.ProducerBuilder]*refCountedProducer
}

func (acbw *acBalancerWrapper) UpdateAddresses(addrs []resolver.Address) {
	acbw.mu.Lock()
	defer acbw.mu.Unlock()
	if len(addrs) <= 0 {
		acbw.ac.cc.removeAddrConn(acbw.ac, errConnDrain)
		return
	}
	if !acbw.ac.tryUpdateAddrs(addrs) {
		cc := acbw.ac.cc
		opts := acbw.ac.scopts
		acbw.ac.mu.Lock()
		// Set old ac.acbw to nil so the Shutdown state update will be ignored
		// by balancer.
		//
		// TODO(bar) the state transition could be wrong when tearDown() old ac
		// and creating new ac, fix the transition.
		acbw.ac.acbw = nil
		acbw.ac.mu.Unlock()
		acState := acbw.ac.getState()
		acbw.ac.cc.removeAddrConn(acbw.ac, errConnDrain)

		if acState == connectivity.Shutdown {
			return
		}

		newAC, err := cc.newAddrConn(addrs, opts)
		if err != nil {
			channelz.Warningf(logger, acbw.ac.channelzID, "acBalancerWrapper: UpdateAddresses: failed to newAddrConn: %v", err)
			return
		}
		acbw.ac = newAC
		newAC.mu.Lock()
		newAC.acbw = acbw
		newAC.mu.Unlock()
		if acState != connectivity.Idle {
			go newAC.connect()
		}
	}
}

func (acbw *acBalancerWrapper) Connect() {
	acbw.mu.Lock()
	defer acbw.mu.Unlock()
	//debug.PrintStack()
	go acbw.ac.connect()
}

func (acbw *acBalancerWrapper) getAddrConn() *addrConn {
	acbw.mu.Lock()
	defer acbw.mu.Unlock()
	return acbw.ac
}

var errSubConnNotReady = status.Error(codes.Unavailable, "SubConn not currently connected")

// NewStream begins a streaming RPC on the addrConn.  If the addrConn is not
// ready, returns errSubConnNotReady.
func (acbw *acBalancerWrapper) NewStream(ctx context.Context, desc *StreamDesc, method string, opts ...CallOption) (ClientStream, error) {
	transport := acbw.ac.getReadyTransport()
	if transport == nil {
		return nil, errSubConnNotReady
	}
	return newNonRetryClientStream(ctx, desc, method, transport, acbw.ac, opts...)
}

// Invoke performs a unary RPC.  If the addrConn is not ready, returns
// errSubConnNotReady.
func (acbw *acBalancerWrapper) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...CallOption) error {
	cs, err := acbw.NewStream(ctx, unaryStreamDesc, method, opts...)
	if err != nil {
		return err
	}
	if err := cs.SendMsg(args); err != nil {
		return err
	}
	return cs.RecvMsg(reply)
}

type refCountedProducer struct {
	producer balancer.Producer
	refs     int    // number of current refs to the producer
	close    func() // underlying producer's close function
}

func (acbw *acBalancerWrapper) GetOrBuildProducer(pb balancer.ProducerBuilder) (balancer.Producer, func()) {
	acbw.mu.Lock()
	defer acbw.mu.Unlock()

	// Look up existing producer from this builder.
	pData := acbw.producers[pb]
	if pData == nil {
		// Not found; create a new one and add it to the producers map.
		p, close := pb.Build(acbw)
		pData = &refCountedProducer{producer: p, close: close}
		acbw.producers[pb] = pData
	}
	// Account for this new reference.
	pData.refs++

	// Return a cleanup function wrapped in a OnceFunc to remove this reference
	// and delete the refCountedProducer from the map if the total reference
	// count goes to zero.
	unref := func() {
		acbw.mu.Lock()
		pData.refs--
		if pData.refs == 0 {
			defer pData.close() // Run outside the acbw mutex
			delete(acbw.producers, pb)
		}
		acbw.mu.Unlock()
	}
	return pData.producer, grpcsync.OnceFunc(unref)
}
