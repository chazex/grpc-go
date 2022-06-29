package main

import (
	"context"
	"fmt"
	echo "google.golang.org/grpc/mytest/name_resolver/proto"
	"log"
	"time"

	"google.golang.org/grpc"
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
