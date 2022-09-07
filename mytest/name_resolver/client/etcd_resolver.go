package main

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"log"
	"time"
)

// EtcdBuilder 实现 resolver.Builder
type EtcdBuilder struct {
	EtcdEndpoints []string
}

// -----------------------------------------------------------------------------

func (b *EtcdBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	fmt.Printf("build Scheme : %s Authority : %s Endpoint : %s  URL : %s\n", target.Scheme, target.Authority, target.Endpoint, target.URL.String())
	// 创建 Etcd 客户端连接
	cfg := clientv3.Config{
		Endpoints:   b.EtcdEndpoints,
		DialTimeout: time.Minute,
	}
	var etcdClient *clientv3.Client
	var err error
	if etcdClient, err = clientv3.New(cfg); err != nil {
		log.Println("create etcd client failed")
		return nil, err
	}
	// 创建 Resolver
	r := ServiceResolver{
		target:     target,
		cc:         cc,
		EtcdClient: etcdClient,
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return &r, nil
}

func (b *EtcdBuilder) Scheme() string {
	return "xxgrpclb"
}

type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Address string `json:"address"`
	Port    int    `json:"port"`
	Color   string `json:"color"`
}

// ServiceResolver 实现 resolver.Resolver 接口
type ServiceResolver struct {
	target     resolver.Target
	cc         resolver.ClientConn
	EtcdClient *clientv3.Client
}

func (r *ServiceResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	// 获取节点列表
	resp, err := r.EtcdClient.Get(context.Background(), fmt.Sprintf("/service/%s", r.target.Endpoint), clientv3.WithPrefix())
	if err != nil {
		log.Println("get service error", err)
	}
	var Address = make([]resolver.Address, 0, resp.Count)
	for _, v := range resp.Kvs {
		fmt.Println(string(v.Key), string(v.Value))
		var serverInfo ServerInfo
		if err := json.Unmarshal(v.Value, &serverInfo); err != nil {
			log.Println(err)
			continue
		}
		// 这里为 Addr 实体添加了 Attributes ， 在负载均衡器器使用颜色来分发流量
		attr := attributes.New("color", serverInfo.Color)
		Address = append(Address, resolver.Address{Addr: fmt.Sprintf("%s:%d", serverInfo.Address, serverInfo.Port), Attributes: attr})
	}
	r.cc.UpdateState(resolver.State{
		Addresses: Address,
	})
}

func (r *ServiceResolver) Close() {
	r.EtcdClient.Close()
}
