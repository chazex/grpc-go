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

package balancer

import "google.golang.org/grpc/connectivity"

// ConnectivityStateEvaluator takes the connectivity states of multiple SubConns
// and returns one aggregated connectivity state.
//
// It's not thread safe.
type ConnectivityStateEvaluator struct {
	numReady            uint64 // Number of addrConns in ready state.
	numConnecting       uint64 // Number of addrConns in connecting state.
	numTransientFailure uint64 // Number of addrConns in transient failure state.
	numIdle             uint64 // Number of addrConns in idle state.
}

// RecordTransition 记录SubConn的状态变更，并且基于此评估聚合状态应该是什么
// - 如果至少一个SubConn是Ready，聚合状态是Ready (感觉和代码逻辑不太一样，至少有一个是Ready则Ready，如果两个都是Ready，那么应该就是Ready。但是代码两个都是Ready的话，-1 和 1相抵，和为0，返回connectivity.TransientFailure)
// - Else if 至少一个SubConn是Connecting，聚合状态是Connecting (由于是Else if，所以前提是没有SubConn是Ready)
// - Else if 至少一个SubConn是Idle，聚合状态是Idle
// - Else if 至少一个SubConn状态是TransientFailure(或者没有SubConn)，聚合状态是Transient Failure
// Shutdown不被考虑在内

// RecordTransition records state change happening in subConn and based on that
// it evaluates what aggregated state should be.
//
//   - If at least one SubConn in Ready, the aggregated state is Ready;
//   - Else if at least one SubConn in Connecting, the aggregated state is Connecting;
//   - Else if at least one SubConn is Idle, the aggregated state is Idle;
//   - Else if at least one SubConn is TransientFailure (or there are no SubConns), the aggregated state is Transient Failure.
//
// Shutdown is not considered.
func (cse *ConnectivityStateEvaluator) RecordTransition(oldState, newState connectivity.State) connectivity.State {
	// Update counters.
	for idx, state := range []connectivity.State{oldState, newState} {
		updateVal := 2*uint64(idx) - 1 // -1 for oldState and +1 for new.
		switch state {
		case connectivity.Ready:
			cse.numReady += updateVal
		case connectivity.Connecting:
			cse.numConnecting += updateVal
		case connectivity.TransientFailure:
			cse.numTransientFailure += updateVal
		case connectivity.Idle:
			cse.numIdle += updateVal
		}
	}
	return cse.CurrentState()
}

// CurrentState returns the current aggregate conn state by evaluating the counters
func (cse *ConnectivityStateEvaluator) CurrentState() connectivity.State {
	// Evaluate.
	if cse.numReady > 0 {
		return connectivity.Ready
	}
	if cse.numConnecting > 0 {
		return connectivity.Connecting
	}
	if cse.numIdle > 0 {
		return connectivity.Idle
	}
	return connectivity.TransientFailure
}
