package main

import (
	"fmt"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"log"
)

// Following is an example name resolver. It includes a
// ResolverBuilder(https://godoc.org/google.golang.org/grpc/resolver#Builder)
// and a Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
//
// A ResolverBuilder is registered for a scheme (in this example, "example" is
// the scheme). When a ClientConn is created for this scheme, the
// ResolverBuilder will be picked to build a Resolver. Note that a new Resolver
// is built for each ClientConn. The Resolver will watch the updates for the
// target, and send updates to the ClientConn.

// exampleResolverBuilder is a
// ResolverBuilder(https://godoc.org/google.golang.org/grpc/resolver#Builder).
const (
	myScheme      = "hsqs" // 看到网上的例子写的是17x，这样跑不过去，因为在提取scheme时，如果开头是以数字，'+', '-', '.'开头的话会直接失败。 net/url/url.go getScheme()
	myServiceName = "resolver.test.hsq.com"

	backendAddr = "localhost:50051"
)

type exampleResolverBuilder struct{}

// Build cc 是 ccResolverWrapper
func (*exampleResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	fmt.Println("builder start.....", target.URL.String())
	r := &exampleResolver{
		target: target,
		cc:     cc,
		addrsStore: map[string][]string{
			myServiceName: {backendAddr},
		},
	}
	// 再Build的时候直接触发了r.ResolveNow()，也就触发了cc.UpdateState()
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}
func (*exampleResolverBuilder) Scheme() string { return myScheme }

// exampleResolver is a
// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type exampleResolver struct {
	target resolver.Target
	// cc ccResolverWrapper
	cc         resolver.ClientConn
	addrsStore map[string][]string
}

func (r *exampleResolver) ResolveNow(o resolver.ResolveNowOptions) {
	// 直接从map中取出对于的addrList
	addrStrs := r.addrsStore[r.target.Endpoint]
	addrs := make([]resolver.Address, len(addrStrs))
	for i, s := range addrStrs {
		log.Println("addr item: ", s)
		addrs[i] = resolver.Address{Addr: s, BalancerAttributes: attributes.New("a", "b").WithValue("c", "d")}
	}
	r.cc.UpdateState(resolver.State{Addresses: addrs}) // 这里可以对State中添加balancer
}

func (*exampleResolver) Close() {}

func init() {
	// Register the example ResolverBuilder. This is usually done in a package's
	// init() function.
	log.Println("register common resolver builder")
	resolver.Register(&exampleResolverBuilder{})
}
