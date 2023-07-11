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

// Package balancer defines APIs for load balancing in gRPC.
// All APIs in this package are experimental.
package balancer

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"

	"google.golang.org/grpc/channelz"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

var (
	// m is a map from name to balancer builder.
	m = make(map[string]Builder)
)

// Register registers the balancer builder to the balancer map. b.Name
// (lowercased) will be used as the name registered with this builder.  If the
// Builder implements ConfigParser, ParseConfig will be called when new service
// configs are received by the resolver, and the result will be provided to the
// Balancer in UpdateClientConnState.
//
// NOTE: this function must only be called during initialization time (i.e. in
// an init() function), and is not thread-safe. If multiple Balancers are
// registered with the same name, the one registered last will take effect.
func Register(b Builder) {
	m[strings.ToLower(b.Name())] = b
}

// unregisterForTesting deletes the balancer with the given name from the
// balancer map.
//
// This function is not thread-safe.
func unregisterForTesting(name string) {
	delete(m, name)
}

func init() {
	internal.BalancerUnregister = unregisterForTesting
}

// Get returns the resolver builder registered with the given name.
// Note that the compare is done in a case-insensitive fashion.
// If no builder is register with the name, nil will be returned.
func Get(name string) Builder {
	if b, ok := m[strings.ToLower(name)]; ok {
		return b
	}
	return nil
}

// SubConn 表示到 gRPC 后端服务的单个连接

// 每个 SubConn 都包含一个地址列表。 （为什么会是地址列表呢？）

// 所有 SubConns 都以 IDLE 状态 启动，并不会尝试去做连接操作。要触发连接的建立，负载均衡器必须调用 Connect方法。
// 如果连接重新进入 IDLE 状态，负载均衡器器必须再次调用 Connect 以触发新的连接尝试。

// gRPC 将尝试按顺序对地址列表进行连接，并在第一次连接成功后停止尝试其余地址。如果连接到所有地址的尝试遇到错误，SubConn 将进入 TRANSIENT_FAILURE 状态，并且有一个退避期，退避期后，然后转换为 IDLE状态（IDLE状态会再次做连接）。

// 一旦建立，如果连接丢失，SubConn 将直接进入IDLE(后面会对IDLE重连)。

// A SubConn represents a single connection to a gRPC backend service.
//
// Each SubConn contains a list of addresses.
//
// All SubConns start in IDLE, and will not try to connect. To trigger the
// connecting, Balancers must call Connect.  If a connection re-enters IDLE,
// Balancers must call Connect again to trigger a new connection attempt.
//
// gRPC will try to connect to the addresses in sequence, and stop trying the
// remainder once the first connection is successful. If an attempt to connect
// to all addresses encounters an error, the SubConn will enter
// TRANSIENT_FAILURE for a backoff period, and then transition to IDLE.
//
// Once established, if a connection is lost, the SubConn will transition
// directly to IDLE.
//
// This interface is to be implemented by gRPC. Users should not need their own
// implementation of this interface. For situations like testing, any
// implementations should embed this interface. This allows gRPC to add new
// methods to this interface.
type SubConn interface {
	// UpdateAddresses 此方法已经过期，放在 ClientConn 接口中了。

	// UpdateAddresses updates the addresses used in this SubConn.
	// gRPC checks if currently-connected address is still in the new list.
	// If it's in the list, the connection will be kept.
	// If it's not in the list, the connection will gracefully closed, and
	// a new connection will be created.
	//
	// This will trigger a state transition for the SubConn.
	//
	// Deprecated: This method is now part of the ClientConn interface and will
	// eventually be removed from here.
	UpdateAddresses([]resolver.Address)
	// Connect starts the connecting for this SubConn.
	Connect()
	// GetOrBuildProducer returns a reference to the existing Producer for this
	// ProducerBuilder in this SubConn, or, if one does not currently exist,
	// creates a new one and returns it.  Returns a close function which must
	// be called when the Producer is no longer needed.
	GetOrBuildProducer(ProducerBuilder) (p Producer, close func())
}

// NewSubConnOptions contains options to create new SubConn.
type NewSubConnOptions struct {
	// CredsBundle is the credentials bundle that will be used in the created
	// SubConn. If it's nil, the original creds from grpc DialOptions will be
	// used.
	//
	// Deprecated: Use the Attributes field in resolver.Address to pass
	// arbitrary data to the credential handshaker.
	CredsBundle credentials.Bundle
	// HealthCheckEnabled indicates whether health check service should be
	// enabled on this SubConn
	HealthCheckEnabled bool
}

// State contains the balancer's state relevant to the gRPC ClientConn.
type State struct {
	// State contains the connectivity state of the balancer, which is used to
	// determine the state of the ClientConn.
	ConnectivityState connectivity.State
	// Picker is used to choose connections (SubConns) for RPCs.
	Picker Picker
}

// ClientConn represents a gRPC ClientConn.
//
// This interface is to be implemented by gRPC. Users should not need a
// brand new implementation of this interface. For the situations like
// testing, the new implementation should embed this interface. This allows
// gRPC to add new methods to this interface.
type ClientConn interface {
	// balancer调用此方法来创建一个SubConn，他不阻塞的去等待连接的建立。

	// NewSubConn is called by balancer to create a new SubConn.
	// It doesn't block and wait for the connections to be established.
	// Behaviors of the SubConn can be controlled by options.
	NewSubConn([]resolver.Address, NewSubConnOptions) (SubConn, error)
	// RemoveSubConn removes the SubConn from ClientConn.
	// The SubConn will be shutdown.
	RemoveSubConn(SubConn)
	// UpdateAddresses updates the addresses used in the passed in SubConn.
	// gRPC checks if the currently connected address is still in the new list.
	// If so, the connection will be kept. Else, the connection will be
	// gracefully closed, and a new connection will be created.
	//
	// This will trigger a state transition for the SubConn.
	UpdateAddresses(SubConn, []resolver.Address)

	// UpdateState 通知gRPC，balancer的内部状态已经改变。
	// gRPC将会更新balancer.ClientConn的连接状态, 并且将会调用新的Picker的Pick()方法去选择新的子连接

	// UpdateState notifies gRPC that the balancer's internal state has
	// changed.
	//
	// gRPC will update the connectivity state of the ClientConn, and will call
	// Pick on the new Picker to pick new SubConns.
	UpdateState(State)

	// ResolveNow balancer调用它，用来通知gRPC去做一个名称解析

	// ResolveNow is called by balancer to notify gRPC to do a name resolving.
	ResolveNow(resolver.ResolveNowOptions)

	// Target returns the dial target for this ClientConn.
	//
	// Deprecated: Use the Target field in the BuildOptions instead.
	Target() string
}

// BuildOptions contains additional information for Build.
type BuildOptions struct {
	// DialCreds is the transport credentials to use when communicating with a
	// remote load balancer server. Balancer implementations which do not
	// communicate with a remote load balancer server can ignore this field.
	DialCreds credentials.TransportCredentials
	// CredsBundle is the credentials bundle to use when communicating with a
	// remote load balancer server. Balancer implementations which do not
	// communicate with a remote load balancer server can ignore this field.
	CredsBundle credentials.Bundle
	// Dialer is the custom dialer to use when communicating with a remote load
	// balancer server. Balancer implementations which do not communicate with a
	// remote load balancer server can ignore this field.
	Dialer func(context.Context, string) (net.Conn, error)
	// Authority is the server name to use as part of the authentication
	// handshake when communicating with a remote load balancer server. Balancer
	// implementations which do not communicate with a remote load balancer
	// server can ignore this field.
	Authority string
	// ChannelzParentID is the parent ClientConn's channelz ID.
	ChannelzParentID *channelz.Identifier
	// CustomUserAgent is the custom user agent set on the parent ClientConn.
	// The balancer should set the same custom user agent if it creates a
	// ClientConn.
	CustomUserAgent string
	// Target contains the parsed address info of the dial target. It is the
	// same resolver.Target as passed to the resolver. See the documentation for
	// the resolver.Target type for details about what it contains.
	Target resolver.Target
}

// Builder creates a balancer.
type Builder interface {
	// Build 这里的 cc 是 balancerWrapper
	// Build creates a new balancer with the ClientConn.
	Build(cc ClientConn, opts BuildOptions) Balancer
	// Name returns the name of balancers built by this builder.
	// It will be used to pick balancers (for example in service config).
	Name() string
}

// ConfigParser parses load balancer configs.
type ConfigParser interface {
	// ParseConfig parses the JSON load balancer config provided into an
	// internal form or returns an error if the config is invalid.  For future
	// compatibility reasons, unknown fields in the config should be ignored.
	ParseConfig(LoadBalancingConfigJSON json.RawMessage) (serviceconfig.LoadBalancingConfig, error)
}

// PickInfo contains additional information for the Pick operation.
type PickInfo struct {
	// FullMethodName is the method name that NewClientStream() is called
	// with. The canonical format is /service/Method.
	FullMethodName string
	// Ctx is the RPC's context, and may contain relevant RPC-level information
	// like the outgoing header metadata.
	Ctx context.Context
}

// DoneInfo contains additional information for done.
type DoneInfo struct {
	// Err is the rpc error the RPC finished with. It could be nil.
	Err error
	// Trailer contains the metadata from the RPC's trailer, if present.
	Trailer metadata.MD
	// BytesSent indicates if any bytes have been sent to the server.
	BytesSent bool
	// BytesReceived indicates if any byte has been received from the server.
	BytesReceived bool
	// ServerLoad is the load received from server. It's usually sent as part of
	// trailing metadata.
	//
	// The only supported type now is *orca_v3.LoadReport.
	ServerLoad interface{}
}

var (
	// ErrNoSubConnAvailable indicates no SubConn is available for pick().
	// gRPC will block the RPC until a new picker is available via UpdateState().
	ErrNoSubConnAvailable = errors.New("no SubConn is available")
	// ErrTransientFailure indicates all SubConns are in TransientFailure.
	// WaitForReady RPCs will block, non-WaitForReady RPCs will fail.
	//
	// Deprecated: return an appropriate error based on the last resolution or
	// connection attempt instead.  The behavior is the same for any non-gRPC
	// status error.
	ErrTransientFailure = errors.New("all SubConns are in TransientFailure")
)

// PickResult contains information related to a connection chosen for an RPC.
type PickResult struct {
	// SubConn is the connection to use for this pick, if its state is Ready.
	// If the state is not Ready, gRPC will block the RPC until a new Picker is
	// provided by the balancer (using ClientConn.UpdateState).  The SubConn
	// must be one returned by ClientConn.NewSubConn.
	SubConn SubConn

	// Done is called when the RPC is completed.  If the SubConn is not ready,
	// this will be called with a nil parameter.  If the SubConn is not a valid
	// type, Done may not be called.  May be nil if the balancer does not wish
	// to be notified when the RPC completes.
	Done func(DoneInfo)

	// Metadata provides a way for LB policies to inject arbitrary per-call
	// metadata. Any metadata returned here will be merged with existing
	// metadata added by the client application.
	//
	// LB policies with child policies are responsible for propagating metadata
	// injected by their children to the ClientConn, as part of Pick().
	Metatada metadata.MD
}

// TransientFailureError returns e.  It exists for backward compatibility and
// will be deleted soon.
//
// Deprecated: no longer necessary, picker errors are treated this way by
// default.
func TransientFailureError(e error) error { return e }

// Picker 被gRPC用来选择SubConn，从而用SubConn发送RPC请求。
// Balancer 每次Balancer的内部状态发生改变的时候，会从它当前快照中（就是依据此刻的状态）生成一个新的picker
// gRPC使用的pickers可以通过ClientConn.UpdateState() 这个方法来更新

// Picker is used by gRPC to pick a SubConn to send an RPC.
// Balancer is expected to generate a new picker from its snapshot every time its
// internal state has changed.
//
// The pickers used by gRPC can be updated by ClientConn.UpdateState().
type Picker interface {
	// Pick returns the connection to use for this RPC and related information.
	//
	// Pick should not block.  If the balancer needs to do I/O or any blocking
	// or time-consuming work to service this call, it should return
	// ErrNoSubConnAvailable, and the Pick call will be repeated by gRPC when
	// the Picker is updated (using ClientConn.UpdateState).
	//
	// If an error is returned:
	//
	// - If the error is ErrNoSubConnAvailable, gRPC will block until a new
	//   Picker is provided by the balancer (using ClientConn.UpdateState).
	//
	// - If the error is a status error (implemented by the grpc/status
	//   package), gRPC will terminate the RPC with the code and message
	//   provided.
	//
	// - For all other errors, wait for ready RPCs will wait, but non-wait for
	//   ready RPCs will be terminated with this error's Error() string and
	//   status code Unavailable.
	Pick(info PickInfo) (PickResult, error)
}

// Balancer 从gRPC获取输入，管理SubConns，收集并聚合连接状态
// 它会生成更新更新gRPC用于做RPC调用时所使用的picker，picker可以获取RPC调用时需要的SubConns
// UpdateClientConnState, ResolverError, UpdateSubConnState, Close 这几个方法可以保证是在同一个goroutine中被同步
// 调用的。picker.Pick没有任何保证，可以随时调用

// Balancer takes input from gRPC, manages SubConns, and collects and aggregates
// the connectivity states.
//
// It also generates and updates the Picker used by gRPC to pick SubConns for RPCs.
//
// UpdateClientConnState, ResolverError, UpdateSubConnState, and Close are
// guaranteed to be called synchronously from the same goroutine.  There's no
// guarantee on picker.Pick, it may be called anytime.
type Balancer interface {
	// UpdateClientConnState is called by gRPC when the state of the ClientConn
	// changes.  If the error returned is ErrBadResolverState, the ClientConn
	// will begin calling ResolveNow on the active name resolver with
	// exponential backoff until a subsequent call to UpdateClientConnState
	// returns a nil error.  Any other errors are currently ignored.
	UpdateClientConnState(ClientConnState) error
	// ResolverError is called by gRPC when the name resolver reports an error.
	ResolverError(error)
	// UpdateSubConnState is called by gRPC when the state of a SubConn
	// changes.
	UpdateSubConnState(SubConn, SubConnState)
	// Close closes the balancer. The balancer is not required to call
	// ClientConn.RemoveSubConn for its existing SubConns.
	Close()
}

// ExitIdler is an optional interface for balancers to implement.  If
// implemented, ExitIdle will be called when ClientConn.Connect is called, if
// the ClientConn is idle.  If unimplemented, ClientConn.Connect will cause
// all SubConns to connect.
//
// Notice: it will be required for all balancers to implement this in a future
// release.
type ExitIdler interface {
	// ExitIdle instructs the LB policy to reconnect to backends / exit the
	// IDLE state, if appropriate and possible.  Note that SubConns that enter
	// the IDLE state will not reconnect until SubConn.Connect is called.
	ExitIdle()
}

// SubConnState describes the state of a SubConn.
type SubConnState struct {
	// ConnectivityState 用于表示SubConn的连接状态
	// ConnectivityState is the connectivity state of the SubConn.
	ConnectivityState connectivity.State
	// 如果ConnectivityState 处于 TransientFailure 状态，则ConnectionError会被设置值，其值用来描述SubConn失败的原因，否则 ConnectionError为nil
	// ConnectionError is set if the ConnectivityState is TransientFailure,
	// describing the reason the SubConn failed.  Otherwise, it is nil.
	ConnectionError error
}

// ClientConnState describes the state of a ClientConn relevant to the
// balancer.
type ClientConnState struct {
	// ResolverState 是Resolver在服务地址变更时触发的服务信息
	ResolverState resolver.State
	// The parsed load balancing configuration returned by the builder's
	// ParseConfig method, if implemented.
	BalancerConfig serviceconfig.LoadBalancingConfig
}

// ErrBadResolverState may be returned by UpdateClientConnState to indicate a
// problem with the provided name resolver data.
var ErrBadResolverState = errors.New("bad resolver state")

// A ProducerBuilder is a simple constructor for a Producer.  It is used by the
// SubConn to create producers when needed.
type ProducerBuilder interface {
	// Build creates a Producer.  The first parameter is always a
	// grpc.ClientConnInterface (a type to allow creating RPCs/streams on the
	// associated SubConn), but is declared as interface{} to avoid a
	// dependency cycle.  Should also return a close function that will be
	// called when all references to the Producer have been given up.
	Build(grpcClientConnInterface interface{}) (p Producer, close func())
}

// A Producer is a type shared among potentially many consumers.  It is
// associated with a SubConn, and an implementation will typically contain
// other methods to provide additional functionality, e.g. configuration or
// subscription registration.
type Producer interface {
}
