package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	echo "google.golang.org/grpc/mytest/name_resolver/proto"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
)

const addr = "localhost:50051"

// ./server -p 10005 -c red
// etcdctl get --prefix /service
var (
	// 服务地址，端口，染色
	ip    = "127.0.0.1"
	port  = flag.Int("p", 8080, "port")
	color = flag.String("c", "green", "color")

	etcd        = []string{"dev8.white.corp.qihoo.net:2379", "127.0.0.1:22379", "127.0.0.1:32379"}
	lease int64 = 10
)

// ServerInfo 服务信息
type ServerInfo struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
	Color   string `json:"color"`
}

type ecServer struct {
	echo.UnimplementedEchoServer
	addr string
}

func (s *ecServer) UnaryEcho(ctx context.Context, req *echo.EchoRequest) (*echo.EchoResponse, error) {
	return &echo.EchoResponse{Message: fmt.Sprintf("%s (from %s)", req.Message, s.addr)}, nil
}

func main() {
	flag.Parse()

	serverInfo := ServerInfo{
		Address: ip,
		Port:    *port,
		Color:   *color,
	}

	etcdClient, grant := serverRegister(serverInfo)
	defer etcdClient.Close()

	// 程序退出时，停止续约
	defer func() {
		etcdClient.Revoke(context.Background(), grant.ID)
		log.Println("停止续约")
	}()

	startServer(serverInfo)
}

func startServer(serverInfo ServerInfo) {
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	echo.RegisterEchoServer(s, &ecServer{addr: fmt.Sprintf("%s:%d-%s", serverInfo.Address, serverInfo.Port, serverInfo.Color)})
	if err := s.Serve(listen); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func serverRegister(serverInfo ServerInfo) (*clientv3.Client, *clientv3.LeaseGrantResponse) {
	// 创建 etcd client
	cfg := clientv3.Config{
		Endpoints:   etcd,
		DialTimeout: time.Minute,
	}
	var etcdClient *clientv3.Client
	var err error
	if etcdClient, err = clientv3.New(cfg); err != nil {
		log.Fatal("create etcd client failed")
	}

	//设置租约时间
	grant, err := etcdClient.Grant(context.Background(), lease)
	if err != nil {
		log.Fatal("craete grant failed", err)
	}
	// put key
	key := "/service/echo/" + uuid.New().String() + "/" + fmt.Sprintf("%s:%d", serverInfo.Address, serverInfo.Port)
	fmt.Println("key: ", key)

	value, err := json.Marshal(serverInfo)
	if err != nil {
		log.Fatal(err)
	}

	_, err = etcdClient.Put(context.Background(), key, string(value), clientv3.WithLease(grant.ID))
	if err != nil {
		log.Fatal("put k-v failed", err)
	}

	log.Printf("服务注册成功\t%s", string(value))

	// 定时续约
	leaseRespChan, err := etcdClient.KeepAlive(context.Background(), grant.ID)
	if err != nil {
		log.Fatal("keep alive failed", err)
	}
	go func(c <-chan *clientv3.LeaseKeepAliveResponse) {
		for v := range c {
			fmt.Println("续约成功", v.ResponseHeader)
		}
		log.Println("停止续约, channel已关闭")
	}(leaseRespChan)

	return etcdClient, grant
}
