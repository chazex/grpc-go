package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc/attributes"
	"log"
	"strings"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
)

const (
	etcdschema = "etcd"
)

// Resolver for grpc client
type Resolver struct {
	schema      string
	EtcdAddrs   []string
	DialTimeout int

	closeCh      chan struct{}
	watchCh      clientv3.WatchChan
	cli          *clientv3.Client
	keyPrifix    string
	srvAddrsList []resolver.Address

	cc     resolver.ClientConn
	logger *zap.Logger
}

// NewResolver create a new resolver.Builder base on etcd
func NewResolver(etcdAddrs []string, logger *zap.Logger) *Resolver {
	return &Resolver{
		schema:      etcdschema,
		EtcdAddrs:   etcdAddrs,
		DialTimeout: 3,
		logger:      logger,
	}
}

// Scheme returns the scheme supported by this resolver.
func (r *Resolver) Scheme() string {
	return r.schema
}

// Build creates a new resolver.Resolver for the given target
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	log.Printf("[self.%s Resolver] Build", etcdschema)
	r.cc = cc

	r.keyPrifix = BuildPrefix(ServerInfo{Name: target.Endpoint, Version: target.Authority})
	fmt.Println("key prefix", r.keyPrifix)
	if _, err := r.start(); err != nil {
		return nil, err
	}
	return r, nil
}

// ResolveNow resolver.Resolver interface
func (r *Resolver) ResolveNow(o resolver.ResolveNowOptions) {
	log.Printf("[self.%s Resolver] 收到 ResolveNow 通知", etcdschema)
}

// Close resolver.Resolver interface
func (r *Resolver) Close() {
	r.closeCh <- struct{}{}
}

// start
func (r *Resolver) start() (chan<- struct{}, error) {
	var err error
	r.cli, err = clientv3.New(clientv3.Config{
		Endpoints:   r.EtcdAddrs,
		DialTimeout: time.Duration(r.DialTimeout) * time.Second,
	})
	if err != nil {
		return nil, err
	}
	//resolver.Register(r)

	r.closeCh = make(chan struct{})

	if err = r.sync(); err != nil {
		return nil, err
	}

	go r.watch()

	return r.closeCh, nil
}

// watch update events
func (r *Resolver) watch() {
	ticker := time.NewTicker(time.Minute * 10)
	r.watchCh = r.cli.Watch(context.Background(), r.keyPrifix, clientv3.WithPrefix())

	for {
		select {
		case <-r.closeCh:
			return
		case res, ok := <-r.watchCh:
			if ok {
				r.update(res.Events)
			}
		case <-ticker.C:
			if err := r.sync(); err != nil {
				r.logger.Error("sync failed", zap.Error(err))
			}
		}
	}
}

var resolveCnt = 0

// update
func (r *Resolver) update(events []*clientv3.Event) {
	resolveCnt += 1
	for _, ev := range events {
		var info ServerInfo
		var err error

		switch ev.Type {
		case mvccpb.PUT:
			info, err = ParseValue(ev.Kv.Value)
			if err != nil {
				continue
			}
			attr := attributes.New("color", info.Color)
			addr := resolver.Address{Addr: fmt.Sprintf("%s:%d", info.Address, info.Port), Attributes: attr, BalancerAttributes: attr}
			if !Exist(r.srvAddrsList, addr) {
				r.srvAddrsList = append(r.srvAddrsList, addr)
				log.Printf("[self.%s Resolver] 监听到新地址\t%s", etcdschema, addr)
				// todo 这里我们可以配置 ServiceConfig ，用来更新负载均衡策略, 也可以配置一些负载均衡的配置
				// 参考 etcd@3.5.4 go.etcd.io/etcd/client/v3/internal/resolver/resolver.go L61
				r.cc.UpdateState(resolver.State{Addresses: r.srvAddrsList, Attributes: attributes.New("state_attr", fmt.Sprintf("put-state-%d", resolveCnt))})
			} else {
				log.Printf("[self.%s Resolver] 监听到已存在服务地址\t%s", etcdschema, addr)
			}
		case mvccpb.DELETE:
			info, err = SplitPath(string(ev.Kv.Key))
			if err != nil {
				continue
			}
			//info, err = ParseValue(ev.Kv.Value)
			//if err != nil {
			//	fmt.Println("删除时间:", err.Error(), string(ev.Kv.Key), string(ev.Kv.Value))
			//	continue
			//}
			attr := attributes.New("color", info.Color)
			addr := resolver.Address{Addr: fmt.Sprintf("%s", info.Address), Attributes: attr, BalancerAttributes: attr}
			fmt.Println("etcd监听到删除服务地址: ", addr)
			log.Printf("[self.%s Resolver] 监听到删除服务地址\t%s", etcdschema, addr)
			if s, ok := Remove(r.srvAddrsList, addr); ok {
				r.srvAddrsList = s
				r.cc.UpdateState(resolver.State{Addresses: r.srvAddrsList, Attributes: attributes.New("state_attr", fmt.Sprintf("del-state-%d", resolveCnt))})
				log.Printf("[self.%s Resolver] 完成删除\t%s", etcdschema, addr)
			}
		}
	}
}

// sync 同步获取所有地址信息
func (r *Resolver) sync() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := r.cli.Get(ctx, r.keyPrifix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	r.srvAddrsList = []resolver.Address{}

	for _, v := range res.Kvs {
		log.Printf("[self.%s Resolver] sync Address\t%s", etcdschema, string(v.Value))
		info, err := ParseValue(v.Value)
		if err != nil {
			continue
		}
		attr := attributes.New("color", info.Color)
		addr := resolver.Address{Addr: fmt.Sprintf("%s:%d", info.Address, info.Port), Attributes: attr}
		r.srvAddrsList = append(r.srvAddrsList, addr)
	}
	r.cc.UpdateState(resolver.State{Addresses: r.srvAddrsList, Attributes: attributes.New("state_attr", fmt.Sprintf("init-del-state-%d", resolveCnt))})
	return nil
}

//func Exist([]resolver.Address, resolver.Address) bool {
//
//}
//
//func ParseValue(data []byte) (ServerInfo, error) {
//	s := ServerInfo{}
//	if err := json.Unmarshal(data, &s); err != nil {
//		return ServerInfo{}, err
//	}
//	return s, nil
//}

func BuildPrefix(info ServerInfo) string {
	if info.Version == "" {
		return fmt.Sprintf("/service/%s/", info.Name)
	}
	return fmt.Sprintf("/service/%s/%s/", info.Name, info.Version)
}

func BuildRegPath(info ServerInfo) string {
	return fmt.Sprintf("%s%s", BuildPrefix(info), info.Address)
}

func ParseValue(value []byte) (ServerInfo, error) {
	info := ServerInfo{}
	if err := json.Unmarshal(value, &info); err != nil {
		return info, err
	}
	return info, nil
}

func SplitPath(path string) (ServerInfo, error) {
	info := ServerInfo{}
	strs := strings.Split(path, "/")
	if len(strs) == 0 {
		return info, errors.New("invalid path")
	}
	info.Address = strs[len(strs)-1]
	return info, nil
}

// Exist helper function
func Exist(l []resolver.Address, addr resolver.Address) bool {
	for i := range l {
		if l[i].Addr == addr.Addr {
			return true
		}
	}
	return false
}

// Remove helper function
func Remove(s []resolver.Address, addr resolver.Address) ([]resolver.Address, bool) {
	for i := range s {
		fmt.Println(s[i].Addr, "==", addr.Addr)
		if s[i].Addr == addr.Addr {
			s[i] = s[len(s)-1]
			return s[:len(s)-1], true
		}
	}
	return nil, false
}

func BuildResolverUrl(app string) string {
	return etcdschema + ":///" + app
}
