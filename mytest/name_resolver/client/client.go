package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/metadata"
	echo "google.golang.org/grpc/mytest/name_resolver/proto"
	"google.golang.org/grpc/resolver"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
)

var (
	etcd = []string{"dev8.white.corp.qihoo.net:2379"}
)

func callUnaryEcho(c echo.EchoClient, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.UnaryEcho(ctx, &echo.EchoRequest{Message: message})
	if err != nil {
		log.Fatalf("could not Echo: %v", err)
	}
	fmt.Println(r.Message)
}

func makeRPCs(cc *grpc.ClientConn, n int) {
	hwc := echo.NewEchoClient(cc)
	for i := 0; i < n; i++ {
		callUnaryEcho(hwc, "this is examples/name_resolving")
	}
}

func main() {
	InitRpc()
	os.Exit(1)

	//passthroughConn, err := grpc.Dial(
	//	// passthrough 也是gRPC内置的一个scheme
	//	fmt.Sprintf("passthrough:///%s", backendAddr), // Dial to "passthrough:///localhost:50051"
	//	grpc.WithInsecure(),
	//	grpc.WithBlock(),
	//)
	//if err != nil {
	//	log.Fatalf("did not connect: %v", err)
	//}
	//defer passthroughConn.Close()
	//
	//fmt.Printf("--- calling helloworld.Greeter/SayHello to \"passthrough:///%s\"\n", backendAddr)
	//makeRPCs(passthroughConn, 10)

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	// defer cancel()
	ctx := context.Background()
	exampleConn, err := grpc.DialContext(
		ctx,
		fmt.Sprintf("%s:///%s", myScheme, myServiceName), // Dial to "17x:///resolver.17x.lixueduan.com"
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"weighted_round_robin"}`),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer exampleConn.Close()

	fmt.Printf("--- calling helloworld.Greeter/SayHello to \"%s:///%s\"\n", myScheme, myServiceName)
	makeRPCs(exampleConn, 10)
}

func InitRpc() {
	// 创建 resolver.Builder
	//b := &EtcdBuilder{
	//	EtcdEndpoints: etcd,
	//}

	b := NewResolver(etcd, nil)
	// 注册 naming resolver
	resolver.Register(b)

	// 注册 负载均衡器
	balancer.Register(base.NewBalancerBuilder("color", ColorPickerBuilder{}, base.Config{HealthCheck: false}))

	// 修改连接的地址，使用自定义的 name resolver
	conn, err := grpc.Dial(fmt.Sprintf("%s:///%s", b.Scheme(), "echo"),
		grpc.WithInsecure(),
		// 配置 loadBalancing 策略
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"color"}`),
	)
	time.Sleep(5 * time.Second)

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	for i := 0; i < 1000; i++ {
		c := echo.NewEchoClient(conn)
		ctx, cancle := context.WithCancel(context.Background())
		defer cancle()
		// 为流量添加颜色
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{"color": "green"}))
		r, err := c.UnaryEcho(ctx, &echo.EchoRequest{Message: "hello"})
		if err != nil {
			log.Fatalf("echo failed :%#v", err.Error())
		}
		log.Print(r.GetMessage())
		time.Sleep(2 * time.Second)
	}

	for i := 0; i < 10; i++ {
		c := echo.NewEchoClient(conn)
		ctx, cancle := context.WithCancel(context.Background())
		defer cancle()

		ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{"color": "red"}))
		r, err := c.UnaryEcho(ctx, &echo.EchoRequest{Message: "hello"})
		if err != nil {
			log.Fatalf("echo failed :%#v", err)
		}
		log.Print(r.GetMessage())
		time.Sleep(time.Second)
	}
}
