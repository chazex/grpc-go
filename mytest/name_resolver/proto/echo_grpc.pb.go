// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.4
// source: echo.proto

package echo

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// EchoClient is the client API for Echo service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type EchoClient interface {
	// UnaryAPI
	UnaryEcho(ctx context.Context, in *EchoRequest, opts ...grpc.CallOption) (*EchoResponse, error)
	// SServerStreaming
	ServerStreamingEcho(ctx context.Context, in *EchoRequest, opts ...grpc.CallOption) (Echo_ServerStreamingEchoClient, error)
	// ClientStreamingE
	ClientStreamingEcho(ctx context.Context, opts ...grpc.CallOption) (Echo_ClientStreamingEchoClient, error)
	// BidirectionalStreaming
	BidirectionalStreamingEcho(ctx context.Context, opts ...grpc.CallOption) (Echo_BidirectionalStreamingEchoClient, error)
}

type echoClient struct {
	// cc 是grpc.ClientConn
	cc grpc.ClientConnInterface
}

func NewEchoClient(cc grpc.ClientConnInterface) EchoClient {
	return &echoClient{cc}
}

func (c *echoClient) UnaryEcho(ctx context.Context, in *EchoRequest, opts ...grpc.CallOption) (*EchoResponse, error) {
	out := new(EchoResponse)
	err := c.cc.Invoke(ctx, "/echo.Echo/UnaryEcho", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *echoClient) ServerStreamingEcho(ctx context.Context, in *EchoRequest, opts ...grpc.CallOption) (Echo_ServerStreamingEchoClient, error) {
	stream, err := c.cc.NewStream(ctx, &Echo_ServiceDesc.Streams[0], "/echo.Echo/ServerStreamingEcho", opts...)
	if err != nil {
		return nil, err
	}
	x := &echoServerStreamingEchoClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Echo_ServerStreamingEchoClient interface {
	Recv() (*EchoResponse, error)
	grpc.ClientStream
}

type echoServerStreamingEchoClient struct {
	grpc.ClientStream
}

func (x *echoServerStreamingEchoClient) Recv() (*EchoResponse, error) {
	m := new(EchoResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *echoClient) ClientStreamingEcho(ctx context.Context, opts ...grpc.CallOption) (Echo_ClientStreamingEchoClient, error) {
	stream, err := c.cc.NewStream(ctx, &Echo_ServiceDesc.Streams[1], "/echo.Echo/ClientStreamingEcho", opts...)
	if err != nil {
		return nil, err
	}
	x := &echoClientStreamingEchoClient{stream}
	return x, nil
}

type Echo_ClientStreamingEchoClient interface {
	Send(*EchoRequest) error
	CloseAndRecv() (*EchoResponse, error)
	grpc.ClientStream
}

type echoClientStreamingEchoClient struct {
	grpc.ClientStream
}

func (x *echoClientStreamingEchoClient) Send(m *EchoRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *echoClientStreamingEchoClient) CloseAndRecv() (*EchoResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(EchoResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *echoClient) BidirectionalStreamingEcho(ctx context.Context, opts ...grpc.CallOption) (Echo_BidirectionalStreamingEchoClient, error) {
	stream, err := c.cc.NewStream(ctx, &Echo_ServiceDesc.Streams[2], "/echo.Echo/BidirectionalStreamingEcho", opts...)
	if err != nil {
		return nil, err
	}
	x := &echoBidirectionalStreamingEchoClient{stream}
	return x, nil
}

type Echo_BidirectionalStreamingEchoClient interface {
	Send(*EchoRequest) error
	Recv() (*EchoResponse, error)
	grpc.ClientStream
}

type echoBidirectionalStreamingEchoClient struct {
	grpc.ClientStream
}

func (x *echoBidirectionalStreamingEchoClient) Send(m *EchoRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *echoBidirectionalStreamingEchoClient) Recv() (*EchoResponse, error) {
	m := new(EchoResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// EchoServer is the server API for Echo service.
// All implementations must embed UnimplementedEchoServer
// for forward compatibility
type EchoServer interface {
	// UnaryAPI
	UnaryEcho(context.Context, *EchoRequest) (*EchoResponse, error)
	// SServerStreaming
	ServerStreamingEcho(*EchoRequest, Echo_ServerStreamingEchoServer) error
	// ClientStreamingE
	ClientStreamingEcho(Echo_ClientStreamingEchoServer) error
	// BidirectionalStreaming
	BidirectionalStreamingEcho(Echo_BidirectionalStreamingEchoServer) error
	mustEmbedUnimplementedEchoServer()
}

// UnimplementedEchoServer must be embedded to have forward compatible implementations.
type UnimplementedEchoServer struct {
}

func (UnimplementedEchoServer) UnaryEcho(context.Context, *EchoRequest) (*EchoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UnaryEcho not implemented")
}
func (UnimplementedEchoServer) ServerStreamingEcho(*EchoRequest, Echo_ServerStreamingEchoServer) error {
	return status.Errorf(codes.Unimplemented, "method ServerStreamingEcho not implemented")
}
func (UnimplementedEchoServer) ClientStreamingEcho(Echo_ClientStreamingEchoServer) error {
	return status.Errorf(codes.Unimplemented, "method ClientStreamingEcho not implemented")
}
func (UnimplementedEchoServer) BidirectionalStreamingEcho(Echo_BidirectionalStreamingEchoServer) error {
	return status.Errorf(codes.Unimplemented, "method BidirectionalStreamingEcho not implemented")
}
func (UnimplementedEchoServer) mustEmbedUnimplementedEchoServer() {}

// UnsafeEchoServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to EchoServer will
// result in compilation errors.
type UnsafeEchoServer interface {
	mustEmbedUnimplementedEchoServer()
}

func RegisterEchoServer(s grpc.ServiceRegistrar, srv EchoServer) {
	s.RegisterService(&Echo_ServiceDesc, srv)
}

func _Echo_UnaryEcho_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EchoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EchoServer).UnaryEcho(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/echo.Echo/UnaryEcho",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(EchoServer).UnaryEcho(ctx, req.(*EchoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Echo_ServerStreamingEcho_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(EchoRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(EchoServer).ServerStreamingEcho(m, &echoServerStreamingEchoServer{stream})
}

type Echo_ServerStreamingEchoServer interface {
	Send(*EchoResponse) error
	grpc.ServerStream
}

type echoServerStreamingEchoServer struct {
	grpc.ServerStream
}

func (x *echoServerStreamingEchoServer) Send(m *EchoResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _Echo_ClientStreamingEcho_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(EchoServer).ClientStreamingEcho(&echoClientStreamingEchoServer{stream})
}

type Echo_ClientStreamingEchoServer interface {
	SendAndClose(*EchoResponse) error
	Recv() (*EchoRequest, error)
	grpc.ServerStream
}

type echoClientStreamingEchoServer struct {
	grpc.ServerStream
}

func (x *echoClientStreamingEchoServer) SendAndClose(m *EchoResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *echoClientStreamingEchoServer) Recv() (*EchoRequest, error) {
	m := new(EchoRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Echo_BidirectionalStreamingEcho_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(EchoServer).BidirectionalStreamingEcho(&echoBidirectionalStreamingEchoServer{stream})
}

type Echo_BidirectionalStreamingEchoServer interface {
	Send(*EchoResponse) error
	Recv() (*EchoRequest, error)
	grpc.ServerStream
}

type echoBidirectionalStreamingEchoServer struct {
	grpc.ServerStream
}

func (x *echoBidirectionalStreamingEchoServer) Send(m *EchoResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *echoBidirectionalStreamingEchoServer) Recv() (*EchoRequest, error) {
	m := new(EchoRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Echo_ServiceDesc is the grpc.ServiceDesc for Echo service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Echo_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "echo.Echo",
	HandlerType: (*EchoServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "UnaryEcho",
			Handler:    _Echo_UnaryEcho_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ServerStreamingEcho",
			Handler:       _Echo_ServerStreamingEcho_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ClientStreamingEcho",
			Handler:       _Echo_ClientStreamingEcho_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "BidirectionalStreamingEcho",
			Handler:       _Echo_BidirectionalStreamingEcho_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "echo.proto",
}
