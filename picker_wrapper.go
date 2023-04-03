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
	"io"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/channelz"
	istatus "google.golang.org/grpc/internal/status"
	"google.golang.org/grpc/internal/transport"
	"google.golang.org/grpc/status"
)

// pickerWrapper 是对balancer.Picker的封装。它在某一个具体的pick动作上阻塞，但是在picker更新上不阻塞。

// pickerWrapper is a wrapper of balancer.Picker. It blocks on certain pick
// actions and unblock when there's a picker update.
type pickerWrapper struct {
	mu         sync.Mutex
	done       bool
	blockingCh chan struct{}
	picker     balancer.Picker
}

func newPickerWrapper() *pickerWrapper {
	return &pickerWrapper{blockingCh: make(chan struct{})}
}

// updatePicker is called by UpdateBalancerState. It unblocks all blocked pick.
func (pw *pickerWrapper) updatePicker(p balancer.Picker) {
	pw.mu.Lock()
	if pw.done {
		pw.mu.Unlock()
		return
	}
	pw.picker = p
	// pw.blockingCh should never be nil.
	close(pw.blockingCh)
	pw.blockingCh = make(chan struct{})
	pw.mu.Unlock()
}

// doneChannelzWrapper performs the following:
//   - increments the calls started channelz counter
//   - wraps the done function in the passed in result to increment the calls
//     failed or calls succeeded channelz counter before invoking the actual
//     done function.
func doneChannelzWrapper(acw *acBalancerWrapper, result *balancer.PickResult) {
	acw.mu.Lock()
	ac := acw.ac
	acw.mu.Unlock()
	ac.incrCallsStarted()
	done := result.Done
	result.Done = func(b balancer.DoneInfo) {
		if b.Err != nil && b.Err != io.EOF {
			ac.incrCallsFailed()
		} else {
			ac.incrCallsSucceeded()
		}
		if done != nil {
			done(b)
		}
	}
}

// 在client调用rpc，做负载均衡时，就是从这里做pick动作，选择连接的。

// pick returns the transport that will be used for the RPC.
// It may block in the following cases:
// - there's no picker
// - the current picker returns ErrNoSubConnAvailable
// - the current picker returns other errors and failfast is false.
// - the subConn returned by the current picker is not READY
// When one of these situations happens, pick blocks until the picker gets updated.
func (pw *pickerWrapper) pick(ctx context.Context, failfast bool, info balancer.PickInfo) (transport.ClientTransport, balancer.PickResult, error) {
	var ch chan struct{}

	var lastPickErr error
	for {
		pw.mu.Lock()
		if pw.done {
			pw.mu.Unlock()
			return nil, balancer.PickResult{}, ErrClientConnClosing
		}

		if pw.picker == nil {
			ch = pw.blockingCh
		}
		if ch == pw.blockingCh {
			// This could happen when either:
			// - pw.picker is nil (the previous if condition), or
			// - has called pick on the current picker.
			pw.mu.Unlock()
			select {
			case <-ctx.Done():
				var errStr string
				if lastPickErr != nil {
					errStr = "latest balancer error: " + lastPickErr.Error()
				} else {
					errStr = ctx.Err().Error()
				}
				switch ctx.Err() {
				case context.DeadlineExceeded:
					return nil, balancer.PickResult{}, status.Error(codes.DeadlineExceeded, errStr)
				case context.Canceled:
					return nil, balancer.PickResult{}, status.Error(codes.Canceled, errStr)
				}
			case <-ch:
				// 如果没有picker，则在这里阻塞(这个应该是出现在初始化的时候)。
				// 在更新picker的时候，会关闭这个channel，所以这里会从阻塞退出来
			}
			continue
		}

		// 这里赋值一下：目的是如果获取http2连接失败了，循环会再次判断ch == pw.blockingCh，如果更新了picker，则二者不相等，再次pick就可以；如果相等说明没更新picker，只能继续等待picker的更新，然后再pick
		ch = pw.blockingCh
		p := pw.picker
		pw.mu.Unlock()

		// 拿到SubConn
		pickResult, err := p.Pick(info)
		if err != nil {
			if err == balancer.ErrNoSubConnAvailable {
				continue
			}
			if st, ok := status.FromError(err); ok {
				// Status error: end the RPC unconditionally with this status.
				// First restrict the code to the list allowed by gRFC A54.
				if istatus.IsRestrictedControlPlaneCode(st) {
					err = status.Errorf(codes.Internal, "received picker error with illegal status: %v", err)
				}
				return nil, balancer.PickResult{}, dropError{error: err}
			}
			// For all other errors, wait for ready RPCs should block and other
			// RPCs should fail with unavailable.
			if !failfast {
				lastPickErr = err
				continue
			}
			return nil, balancer.PickResult{}, status.Error(codes.Unavailable, err.Error())
		}

		acw, ok := pickResult.SubConn.(*acBalancerWrapper)
		if !ok {
			logger.Errorf("subconn returned from pick is type %T, not *acBalancerWrapper", pickResult.SubConn)
			continue
		}
		if t := acw.getAddrConn().getReadyTransport(); t != nil {
			if channelz.IsOn() {
				doneChannelzWrapper(acw, &pickResult)
				return t, pickResult, nil
			}
			// 成功拿到http2连接，函数返回
			return t, pickResult, nil
		}
		if pickResult.Done != nil {
			// Calling done with nil error, no bytes sent and no bytes received.
			// DoneInfo with default value works.
			pickResult.Done(balancer.DoneInfo{})
		}
		logger.Infof("blockingPicker: the picked transport is not ready, loop back to repick")
		// If ok == false, ac.state is not READY.
		// A valid picker always returns READY subConn. This means the state of ac
		// just changed, and picker will be updated shortly.
		// continue back to the beginning of the for loop to repick.
	}
}

func (pw *pickerWrapper) close() {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	if pw.done {
		return
	}
	pw.done = true
	close(pw.blockingCh)
}

// dropError is a wrapper error that indicates the LB policy wishes to drop the
// RPC and not retry it.
type dropError struct {
	error
}
