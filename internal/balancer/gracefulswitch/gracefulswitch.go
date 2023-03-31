/*
 *
 * Copyright 2022 gRPC authors.
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

// Package gracefulswitch implements a graceful switch load balancer.
package gracefulswitch

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

var errBalancerClosed = errors.New("gracefulSwitchBalancer is closed")
var _ balancer.Balancer = (*Balancer)(nil)

// NewBalancer returns a graceful switch Balancer.
func NewBalancer(cc balancer.ClientConn, opts balancer.BuildOptions) *Balancer {
	return &Balancer{
		cc:    cc,
		bOpts: opts,
	}
}

// gracefulswitch.Balancer是一个用来优雅切换balancer的工具。它实现了balancer.Balancer接口。

// Balancer is a utility to gracefully switch from one balancer to
// a new balancer. It implements the balancer.Balancer interface.
type Balancer struct {
	bOpts balancer.BuildOptions
	// 这里的cc是*ccBalancerWrapper
	cc balancer.ClientConn

	// mu protects the following fields and all fields within balancerCurrent
	// and balancerPending. mu does not need to be held when calling into the
	// child balancers, as all calls into these children happen only as a direct
	// result of a call into the gracefulSwitchBalancer, which are also
	// guaranteed to be synchronous. There is one exception: an UpdateState call
	// from a child balancer when current and pending are populated can lead to
	// calling Close() on the current. To prevent that racing with an
	// UpdateSubConnState from the channel, we hold currentMu during Close and
	// UpdateSubConnState calls.
	mu sync.Mutex
	// 当前使用的balancer
	balancerCurrent *balancerWrapper
	// 新的balancer（将要上位的balancer）
	balancerPending *balancerWrapper
	closed          bool // set to true when this balancer is closed

	// currentMu must be locked before mu. This mutex guards against this
	// sequence of events: UpdateSubConnState() called, finds the
	// balancerCurrent, gives up lock, updateState comes in, causes Close() on
	// balancerCurrent before the UpdateSubConnState is called on the
	// balancerCurrent.
	currentMu sync.Mutex
}

// 交换balancerPending 和 balancerCurrent，并关闭balancerCurrent

// swap swaps out the current lb with the pending lb and updates the ClientConn.
// The caller must hold gsb.mu.
func (gsb *Balancer) swap() {
	// 更新picker
	gsb.cc.UpdateState(gsb.balancerPending.lastState)
	// 交换
	cur := gsb.balancerCurrent
	gsb.balancerCurrent = gsb.balancerPending
	gsb.balancerPending = nil
	go func() {
		gsb.currentMu.Lock()
		defer gsb.currentMu.Unlock()
		// 关闭之前的balancer
		cur.Close()
	}()
}

// Helper function that checks if the balancer passed in is current or pending.
// The caller must hold gsb.mu.
func (gsb *Balancer) balancerCurrentOrPending(bw *balancerWrapper) bool {
	return bw == gsb.balancerCurrent || bw == gsb.balancerPending
}

// SwitchTo initializes the graceful switch process, which completes based on
// connectivity state changes on the current/pending balancer. Thus, the switch
// process is not complete when this method returns. This method must be called
// synchronously alongside the rest of the balancer.Balancer methods this
// Graceful Switch Balancer implements.
func (gsb *Balancer) SwitchTo(builder balancer.Builder) error {
	gsb.mu.Lock()
	if gsb.closed {
		gsb.mu.Unlock()
		return errBalancerClosed
	}
	bw := &balancerWrapper{
		gsb: gsb,
		lastState: balancer.State{
			ConnectivityState: connectivity.Connecting,
			Picker:            base.NewErrPicker(balancer.ErrNoSubConnAvailable),
		},
		subconns: make(map[balancer.SubConn]bool),
	}
	balToClose := gsb.balancerPending // nil if there is no pending balancer
	if gsb.balancerCurrent == nil {
		// 如果当前balancer为nil，这将当前balancer指向新的*balancerWrapper. 这一般是在第一次初始化的时候
		gsb.balancerCurrent = bw
	} else {
		// 如果当前balancer不为nil，则将新的*balancerWrapper,指向pending
		// 疑问：什么时候交换Pending和Current呢?
		gsb.balancerPending = bw
	}
	gsb.mu.Unlock()
	// 将老的pending状态的balancer关闭掉
	balToClose.Close()

	//调用balancer.Builder.Build()方法，构建balancer
	// This function takes a builder instead of a balancer because builder.Build
	// can call back inline, and this utility needs to handle the callbacks.
	newBalancer := builder.Build(bw, gsb.bOpts)
	if newBalancer == nil {
		// 构建失败
		// This is illegal and should never happen; we clear the balancerWrapper
		// we were constructing if it happens to avoid a potential panic.
		gsb.mu.Lock()
		if gsb.balancerPending != nil {
			gsb.balancerPending = nil
		} else {
			// 进入这个分值，是因为balancerPending为nil，所以前面新建的balancerWrapper是直接给了balancerCurrennt，这一般是在第一次初始化的时候
			gsb.balancerCurrent = nil
		}
		gsb.mu.Unlock()
		return balancer.ErrBadResolverState
	}

	// This write doesn't need to take gsb.mu because this field never gets read
	// or written to on any calls from the current or pending. Calls from grpc
	// to this balancer are guaranteed to be called synchronously, so this
	// bw.Balancer field will never be forwarded to until this SwitchTo()
	// function returns.
	bw.Balancer = newBalancer
	return nil
}

// Returns nil if the graceful switch balancer is closed.
func (gsb *Balancer) latestBalancer() *balancerWrapper {
	gsb.mu.Lock()
	defer gsb.mu.Unlock()
	if gsb.balancerPending != nil {
		return gsb.balancerPending
	}
	// 走到这里，一般是第一次初始化的时候
	return gsb.balancerCurrent
}

// 参数state中包含resolver获取到的地址列表 state.ResolverState

// UpdateClientConnState forwards the update to the latest balancer created.
func (gsb *Balancer) UpdateClientConnState(state balancer.ClientConnState) error {
	// 获取最新的balancer实现，这个实现是被*balancerWrapper包装过的。
	// The resolver data is only relevant to the most recent LB Policy.
	balToUpdate := gsb.latestBalancer()
	if balToUpdate == nil {
		return errBalancerClosed
	}

	// *balancerWrapper自己没有实现UpdateClientConnState方法，这里调用的是*balancerWrapper里面包装的具体的balancer的UpdateClientConnState()方法，比如*baseBalancer。
	// *balancerWrapper.Balancer的赋值是发生在SwitchTo事件中，此事件通过全局balancer.Builder表，获取到balancer.Builder，并创建balancer。
	// Perform this call without gsb.mu to prevent deadlocks if the child calls
	// back into the channel. The latest balancer can never be closed during a
	// call from the channel, even without gsb.mu held.
	return balToUpdate.UpdateClientConnState(state)
}

// ResolverError forwards the error to the latest balancer created.
func (gsb *Balancer) ResolverError(err error) {
	// The resolver data is only relevant to the most recent LB Policy.
	balToUpdate := gsb.latestBalancer()
	if balToUpdate == nil {
		return
	}
	// Perform this call without gsb.mu to prevent deadlocks if the child calls
	// back into the channel. The latest balancer can never be closed during a
	// call from the channel, even without gsb.mu held.
	balToUpdate.ResolverError(err)
}

// ExitIdle forwards the call to the latest balancer created.
//
// If the latest balancer does not support ExitIdle, the subConns are
// re-connected to manually.
func (gsb *Balancer) ExitIdle() {
	balToUpdate := gsb.latestBalancer()
	if balToUpdate == nil {
		return
	}
	// There is no need to protect this read with a mutex, as the write to the
	// Balancer field happens in SwitchTo, which completes before this can be
	// called.
	if ei, ok := balToUpdate.Balancer.(balancer.ExitIdler); ok {
		ei.ExitIdle()
		return
	}
	gsb.mu.Lock()
	defer gsb.mu.Unlock()
	for sc := range balToUpdate.subconns {
		sc.Connect()
	}
}

// UpdateSubConnState forwards the update to the appropriate child.
func (gsb *Balancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	gsb.currentMu.Lock()
	defer gsb.currentMu.Unlock()
	gsb.mu.Lock()
	// Forward update to the appropriate child.  Even if there is a pending
	// balancer, the current balancer should continue to get SubConn updates to
	// maintain the proper state while the pending is still connecting.
	var balToUpdate *balancerWrapper
	if gsb.balancerCurrent != nil && gsb.balancerCurrent.subconns[sc] {
		balToUpdate = gsb.balancerCurrent
	} else if gsb.balancerPending != nil && gsb.balancerPending.subconns[sc] {
		balToUpdate = gsb.balancerPending
	}
	gsb.mu.Unlock()
	if balToUpdate == nil {
		// SubConn belonged to a stale lb policy that has not yet fully closed,
		// or the balancer was already closed.
		return
	}
	balToUpdate.UpdateSubConnState(sc, state)
}

// Close closes any active child balancers.
func (gsb *Balancer) Close() {
	gsb.mu.Lock()
	gsb.closed = true
	currentBalancerToClose := gsb.balancerCurrent
	gsb.balancerCurrent = nil
	pendingBalancerToClose := gsb.balancerPending
	gsb.balancerPending = nil
	gsb.mu.Unlock()

	currentBalancerToClose.Close()
	pendingBalancerToClose.Close()
}

// balancerWrapper wraps a balancer.Balancer, and overrides some Balancer
// methods to help cleanup SubConns created by the wrapped balancer.
//
// It implements the balancer.ClientConn interface and is passed down in that
// capacity to the wrapped balancer. It maintains a set of subConns created by
// the wrapped balancer and calls from the latter to create/update/remove
// SubConns update this set before being forwarded to the parent ClientConn.
// State updates from the wrapped balancer can result in invocation of the
// graceful switch logic.
type balancerWrapper struct {
	balancer.Balancer
	gsb *Balancer

	lastState balancer.State
	subconns  map[balancer.SubConn]bool // subconns created by this balancer
}

func (bw *balancerWrapper) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	if state.ConnectivityState == connectivity.Shutdown {
		bw.gsb.mu.Lock()
		delete(bw.subconns, sc)
		bw.gsb.mu.Unlock()
	}
	// There is no need to protect this read with a mutex, as the write to the
	// Balancer field happens in SwitchTo, which completes before this can be
	// called.
	bw.Balancer.UpdateSubConnState(sc, state)
}

// Close closes the underlying LB policy and removes the subconns it created. bw
// must not be referenced via balancerCurrent or balancerPending in gsb when
// called. gsb.mu must not be held.  Does not panic with a nil receiver.
func (bw *balancerWrapper) Close() {
	// before Close is called.
	if bw == nil {
		return
	}
	// There is no need to protect this read with a mutex, as Close() is
	// impossible to be called concurrently with the write in SwitchTo(). The
	// callsites of Close() for this balancer in Graceful Switch Balancer will
	// never be called until SwitchTo() returns.
	bw.Balancer.Close()
	bw.gsb.mu.Lock()
	for sc := range bw.subconns {
		bw.gsb.cc.RemoveSubConn(sc)
	}
	bw.gsb.mu.Unlock()
}

// 参数state是聚合状态

func (bw *balancerWrapper) UpdateState(state balancer.State) {
	// Hold the mutex for this entire call to ensure it cannot occur
	// concurrently with other updateState() calls. This causes updates to
	// lastState and calls to cc.UpdateState to happen atomically.
	bw.gsb.mu.Lock()
	defer bw.gsb.mu.Unlock()
	bw.lastState = state

	if !bw.gsb.balancerCurrentOrPending(bw) {
		return
	}

	// 因为balancer中的每一个子链接状态变更，或者resolver发现了新的连接并且负载均衡策略不改变，会都进入到baseBalancer.UpdateSubConnState()中，也就会调用当前发方法，此时bw == bw.gsb.balancerCurrent
	if bw == bw.gsb.balancerCurrent {
		// 如果当前current balancer退出READY状态，并且存在一个pending banlancer,
		// 不太理解：这里只能证明只有一个子链接出现了问题,而不判断pending balancer的状态，直接切换，合适吗？

		// In the case that the current balancer exits READY, and there is a pending
		// balancer, you can forward the pending balancer's cached State up to
		// ClientConn and swap the pending into the current. This is because there
		// is no reason to gracefully switch from and keep using the old policy as
		// the ClientConn is not connected to any backends.
		if state.ConnectivityState != connectivity.Ready && bw.gsb.balancerPending != nil {
			bw.gsb.swap()
			return
		}

		// 走到这里的情况是：balancer建立了一个新的READY的连接(因为连接建立是异步的)，所以要更新picker.
		fmt.Println("更新picker")

		// Even if there is a pending balancer waiting to be gracefully switched to,
		// continue to forward current balancer updates to the Client Conn. Ignoring
		// state + picker from the current would cause undefined behavior/cause the
		// system to behave incorrectly from the current LB policies perspective.
		// Also, the current LB is still being used by grpc to choose SubConns per
		// RPC, and thus should use the most updated form of the current balancer.
		bw.gsb.cc.UpdateState(state)
		return
	}

	// 代码走到这里，说明当前的balancer处于pending状态（因为它不等于currentBalancer）
	// 此时如果current的状态不是Ready，而当前balancer状态不是Connecting，那么我们做交换，将当前balancer，设置为current

	// This method is now dealing with a state update from the pending balancer.
	// If the current balancer is currently in a state other than READY, the new
	// policy can be swapped into place immediately. This is because there is no
	// reason to gracefully switch from and keep using the old policy as the
	// ClientConn is not connected to any backends.
	if state.ConnectivityState != connectivity.Connecting || bw.gsb.balancerCurrent.lastState.ConnectivityState != connectivity.Ready {
		bw.gsb.swap()
	}
}

func (bw *balancerWrapper) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	bw.gsb.mu.Lock()
	// 既不是Current 也不是 Pending，那么这个应该是过期的
	if !bw.gsb.balancerCurrentOrPending(bw) {
		bw.gsb.mu.Unlock()
		return nil, fmt.Errorf("%T at address %p that called NewSubConn is deleted", bw, bw)
	}
	bw.gsb.mu.Unlock()

	// 建立连接, bw.gsb.cc.NewSubConn()，实际上调用的是ccBalancerWrapper.NewSubConn()。返回的是*acBalancerWrapper,他内部包含*addrConn
	sc, err := bw.gsb.cc.NewSubConn(addrs, opts)
	if err != nil {
		return nil, err
	}
	bw.gsb.mu.Lock()
	if !bw.gsb.balancerCurrentOrPending(bw) { // balancer was closed during this call
		bw.gsb.cc.RemoveSubConn(sc)
		bw.gsb.mu.Unlock()
		return nil, fmt.Errorf("%T at address %p that called NewSubConn is deleted", bw, bw)
	}
	// 保留一份*acBalancerWrapper
	bw.subconns[sc] = true
	bw.gsb.mu.Unlock()
	return sc, nil
}

func (bw *balancerWrapper) ResolveNow(opts resolver.ResolveNowOptions) {
	// Ignore ResolveNow requests from anything other than the most recent
	// balancer, because older balancers were already removed from the config.
	if bw != bw.gsb.latestBalancer() {
		return
	}
	// 通知grpc去做一次域名解析
	bw.gsb.cc.ResolveNow(opts)
}

func (bw *balancerWrapper) RemoveSubConn(sc balancer.SubConn) {
	bw.gsb.mu.Lock()
	if !bw.gsb.balancerCurrentOrPending(bw) {
		bw.gsb.mu.Unlock()
		return
	}
	bw.gsb.mu.Unlock()
	bw.gsb.cc.RemoveSubConn(sc)
}

func (bw *balancerWrapper) UpdateAddresses(sc balancer.SubConn, addrs []resolver.Address) {
	bw.gsb.mu.Lock()
	if !bw.gsb.balancerCurrentOrPending(bw) {
		bw.gsb.mu.Unlock()
		return
	}
	bw.gsb.mu.Unlock()
	bw.gsb.cc.UpdateAddresses(sc, addrs)
}

func (bw *balancerWrapper) Target() string {
	return bw.gsb.cc.Target()
}
