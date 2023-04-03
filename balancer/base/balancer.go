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

package base

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

var logger = grpclog.Component("balancer")

type baseBuilder struct {
	name          string
	pickerBuilder PickerBuilder
	config        Config
}

// balancer.Builder.Build()方法，在SwitchTo事件中调用的。
// cc参数是新建的*balancerWrapper

func (bb *baseBuilder) Build(cc balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	bal := &baseBalancer{
		cc:            cc,
		pickerBuilder: bb.pickerBuilder,

		subConns: resolver.NewAddressMap(),
		scStates: make(map[balancer.SubConn]connectivity.State),
		csEvltr:  &balancer.ConnectivityStateEvaluator{},
		config:   bb.config,
		state:    connectivity.Connecting,
	}
	// Initialize picker to a picker that always returns
	// ErrNoSubConnAvailable, because when state of a SubConn changes, we
	// may call UpdateState with this picker.
	bal.picker = NewErrPicker(balancer.ErrNoSubConnAvailable)
	return bal
}

func (bb *baseBuilder) Name() string {
	return bb.name
}

type baseBalancer struct {
	// cc参数是*balancerWrapper，*balancerWrapper是对当前baseBalancer的包装
	cc            balancer.ClientConn
	pickerBuilder PickerBuilder

	csEvltr *balancer.ConnectivityStateEvaluator

	// 聚合状态
	state connectivity.State

	subConns *resolver.AddressMap
	// baseBalancer 管理连接的地方
	scStates map[balancer.SubConn]connectivity.State

	// 用于从Ready状态的连接中，挑选出可用的连接
	picker balancer.Picker
	config Config

	resolverErr error // the last error reported by the resolver; cleared on successful resolution
	// 最后一次的连接错误，在离开状态TransientFailure时，错误被清楚
	connErr error // the last connection error; cleared upon leaving TransientFailure
}

func (b *baseBalancer) ResolverError(err error) {
	b.resolverErr = err
	if b.subConns.Len() == 0 {
		b.state = connectivity.TransientFailure
	}

	if b.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}
	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

// UpdateClientConnState 每个balancer都需要实现这个方法，当resolver发现服务地址有变化时，会通过发送*ccStateUpdate事件，来调用该方法。
// 参数s包含了resolver获得的服务地址列表，以及负载均衡配置

func (b *baseBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	d, _ := s.ResolverState.Attributes.Value("state_attr").(string)
	fmt.Printf("触发更新\t%p\t%d\t%v\n", b, b.subConns.Len(), d)

	// TODO: handle s.ResolverState.ServiceConfig?
	if logger.V(2) {
		logger.Info("base.baseBalancer: got new ClientConn state: ", s)
	}
	// Successful resolution; clear resolver error and ensure we return nil.
	b.resolverErr = nil
	// addrsSet is the set converted from addrs, it's used for quick lookup of an address.
	addrsSet := resolver.NewAddressMap()
	// s.ResolverState.Addresses 应该是命名解析新获取到的地址列表
	for _, a := range s.ResolverState.Addresses {
		addrsSet.Set(a, nil)
		// 这里需要注意一点：代码在这里执行的时候，其实是一个新的Balancer，里面的连接(b.subConns)是空的。
		// 因为每次域名解析得到新的地址列表的时候，都会调用BalancerBuilder.Build()重新创建一个Balancer，所以它的subConns是空的，但是这里面还是get了一下看有没有，我感觉这里是怕重复
		// 所以每此新生成的Balancer，其内部的连接都是新建的。

		// 上面的结论是错误的。 在每次域名解析得到新的地址列表的时候，在balancer_name没有发生变化的时候，是不会调用BalancerBuilder.Build()的。
		// 也就是说，只有负载均衡策略发生了改变，才会调用BalancerBuilder.Build()，重新生成Balancer。
		if _, ok := b.subConns.Get(a); !ok {
			// 发现新地址: 因为地址没有在连接列表中。
			// 开始创建连接. 这里的b.cc.NewSubConn()，实际上是*balancerWrapper.NewSubConn()经过一系列周转，最终是到了grpc.ClientConn.newAddrConn()
			// 这里并没有真正的创建连接，返回的sc是*acBalancerWrapper,他内部包含*addrConn。
			// a is a new address (not existing in b.subConns).
			sc, err := b.cc.NewSubConn([]resolver.Address{a}, balancer.NewSubConnOptions{HealthCheckEnabled: b.config.HealthCheck})
			if err != nil {
				logger.Warningf("base.baseBalancer: failed to create new SubConn: %v", err)
				continue
			}
			fmt.Println("balancer发现新的地址\t创建新连接", a.Addr, a.Attributes)
			b.subConns.Set(a, sc)
			b.scStates[sc] = connectivity.Idle
			// 这里是为idle状态的数量+1
			b.csEvltr.RecordTransition(connectivity.Shutdown, connectivity.Idle)

			// 正式创建连接（内部起一个协程，来调用到*addrConn.connect()方法）
			sc.Connect()
		}
	}

	// 清理过期的连接。比如：名称解析返回的地址列表中，删除了某一个地址，此时连接还在b.subConns中，此时应该把连接一处。

	for _, a := range b.subConns.Keys() {
		sci, _ := b.subConns.Get(a)
		sc := sci.(balancer.SubConn)
		// a was removed by resolver.
		if _, ok := addrsSet.Get(a); !ok {
			b.cc.RemoveSubConn(sc)
			b.subConns.Delete(a)
			// Keep the state of this sc in b.scStates until sc's state becomes Shutdown.
			// The entry will be deleted in UpdateSubConnState.
		}
	}
	// If resolver state contains no addresses, return an error so ClientConn
	// will trigger re-resolve. Also records this as an resolver error, so when
	// the overall state turns transient failure, the error message will have
	// the zero address information.
	if len(s.ResolverState.Addresses) == 0 {
		b.ResolverError(errors.New("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	b.regeneratePicker()
	// *balancerWrapper.UpdateState()
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
	return nil
}

// mergeErrors builds an error from the last connection error and the last
// resolver error.  Must only be called if b.state is TransientFailure.
func (b *baseBalancer) mergeErrors() error {
	// connErr must always be non-nil unless there are no SubConns, in which
	// case resolverErr must be non-nil.
	if b.connErr == nil {
		return fmt.Errorf("last resolver error: %v", b.resolverErr)
	}
	if b.resolverErr == nil {
		return fmt.Errorf("last connection error: %v", b.connErr)
	}
	return fmt.Errorf("last connection error: %v; last resolver error: %v", b.connErr, b.resolverErr)
}

// regeneratePicker 重新生成一个picker
// picker生成策略
//  - 如果balancer处于TransientFailure状态，则生成一个errPicker
//  - 否则，通过pickerBuilder，针对所有Ready状态的SubConns构建一个新的picker

// regeneratePicker takes a snapshot of the balancer, and generates a picker
// from it. The picker is
//   - errPicker if the balancer is in TransientFailure,
//   - built by the pickerBuilder with all READY SubConns otherwise.
func (b *baseBalancer) regeneratePicker() {
	fmt.Println("执行了 regeneratePicker")
	if b.state == connectivity.TransientFailure {
		b.picker = NewErrPicker(b.mergeErrors())
		return
	}
	readySCs := make(map[balancer.SubConn]SubConnInfo)

	// 遍历连接池中的连接，找出状态为Ready的，交给Picker用来做负载均衡, Picker对这些可用的连接根据自身的逻辑做选择。

	// Filter out all ready SCs from full subConn map.
	for _, addr := range b.subConns.Keys() {
		sci, _ := b.subConns.Get(addr)
		sc := sci.(balancer.SubConn)
		if st, ok := b.scStates[sc]; ok && st == connectivity.Ready {
			readySCs[sc] = SubConnInfo{Address: addr}
		}
	}

	// 根据所有可用的连接，调用pickerBuilder.Build()方法，重新生成picker
	b.picker = b.pickerBuilder.Build(PickerBuildInfo{ReadySCs: readySCs})
}

// SubConn连接状态发生变化时，会调用此方法
// 比如Resolver发现新的地址，此时负载均衡会依据新的服务地址创建新的连接，创建连接之前，把连接状态设置为Connecting

func (b *baseBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	// s 表示SubConn的新连接状态
	s := state.ConnectivityState
	if logger.V(2) {
		logger.Infof("base.baseBalancer: handle SubConn state change: %p, %v", sc, s)
	}
	// oldS 表示SubConn的原始连接状态
	oldS, ok := b.scStates[sc]
	if !ok {
		// 连接不存在
		if logger.V(2) {
			logger.Infof("base.baseBalancer: got state changes for an unknown SubConn: %p, %v", sc, s)
		}
		return
	}
	if oldS == connectivity.TransientFailure &&
		(s == connectivity.Connecting || s == connectivity.Idle) {
		// Once a subconn enters TRANSIENT_FAILURE, ignore subsequent IDLE or
		// CONNECTING transitions to prevent the aggregated state from being
		// always CONNECTING when many backends exist but are all down.
		if s == connectivity.Idle {
			// 这里发生在连接失败，重试的过程中。连接失败后，resetTransport()函数会先设置状态为TransientFailure，然后再设置为Idle状态
			sc.Connect()
		}
		return
	}
	// 更新subConn状态
	b.scStates[sc] = s
	switch s {
	case connectivity.Idle:
		// 状态还是Idle，再次连接
		sc.Connect()
	case connectivity.Shutdown:
		// 当一个地址被resolver移除，此时会调用removeSubConn，但是scStates中关于这个连接的状态还在保持。 我们在这里移除这个subConn的状态。
		// When an address was removed by resolver, b called RemoveSubConn but
		// kept the sc's state in scStates. Remove state for this sc here.
		delete(b.scStates, sc)
	case connectivity.TransientFailure:
		// Save error to be reported via picker.
		b.connErr = state.ConnectionError
	}

	b.state = b.csEvltr.RecordTransition(oldS, s)
	fmt.Printf("聚合状态\t老状态： %v\t新状态：%v\t聚合状态\t%v\n", oldS, s, b.state)

	// s == （connectivity.Ready) != (oldS == connectivity.Ready) ：这段代码解释： 新状态和老状态，只有一个是Ready在为True，都是Ready，或都不是Ready时，为false

	// Regenerate picker when one of the following happens:
	//  - this sc entered or left ready
	//  - the aggregated state of balancer is TransientFailure
	//    (may need to update error message)
	if (s == connectivity.Ready) != (oldS == connectivity.Ready) ||
		b.state == connectivity.TransientFailure {
		//   新老状态只有一个为Ready (这意味着连接状态发生了变化，从Ready变成非Ready，或者从非Ready变成了Ready，这两种情况都要更新Picker，从而把失败的连接从picker中清除掉，后面再次连接成功后，会再生成picker的。)
		// 或者
		//   聚合状态为TransientFailure 时，重新生成Picker
		b.regeneratePicker()
	}
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
}

// 关闭balancer：发生在CurrentBalancer和PendingBalancer交换时，将CurrentBalancer关闭。
// 不过对于baseBalancer不需要关闭，所以关闭是个空函数，因为它没有需要清理的内部状态；它也不需要调用为SubConns调用RemoveSubConn

// Close is a nop because base balancer doesn't have internal state to clean up,
// and it doesn't need to call RemoveSubConn for the SubConns.
func (b *baseBalancer) Close() {
}

// ExitIdle is a nop because the base balancer attempts to stay connected to
// all SubConns at all times.
func (b *baseBalancer) ExitIdle() {
}

// NewErrPicker returns a Picker that always returns err on Pick().
func NewErrPicker(err error) balancer.Picker {
	return &errPicker{err: err}
}

// NewErrPickerV2 is temporarily defined for backward compatibility reasons.
//
// Deprecated: use NewErrPicker instead.
var NewErrPickerV2 = NewErrPicker

type errPicker struct {
	err error // Pick() always returns this err.
}

func (p *errPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{}, p.err
}
