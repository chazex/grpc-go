package main

import (
	"errors"
	"fmt"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/metadata"
	"log"
)

const colorBalancerPicker = "color"

type ColorPickerBuilder struct{}

func (cpb ColorPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	log.Printf("[self.%s BalancerPicker] Builder", colorBalancerPicker)

	var scs = map[string][]balancer.SubConn{}
	for k, v := range info.ReadySCs {
		color, ok := v.Address.Attributes.Value("color").(string)
		if ok {
			scs[color] = append(scs[color], k)
		} else {
			log.Printf("error\t没找到对应的颜色\t%s", color)
			scs[""] = append(scs[""], k)
		}
	}
	return &ColorPicker{scs: scs}

}

// ColorPicker 负载均衡器
type ColorPicker struct {
	scs map[string][]balancer.SubConn
}

func (cb *ColorPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	log.Printf("[self.%s BalancerPicker] Pick\t%v", colorBalancerPicker, cb.scs)
	// 获取请求流量颜色
	md, ok := metadata.FromOutgoingContext(info.Ctx)
	color := ""
	if ok {
		colors := md.Get("color")
		if len(colors) != 0 {
			color = colors[0]
		}
	}
	fmt.Println(color)
	// 选择节点  这里可以添加容错逻辑，带颜色的节点不存在时，使用没有颜色的节点来处理流量
	sclist := cb.scs[color]

	log.Printf("[self.%s BalancerPicker] Pick Result\t%+v", colorBalancerPicker, sclist) // 在服务下线后，etcd还没有删除key时，这里为啥立刻就更新了。  因为执行了baseBalancer.regeneratePicker()，应该发现subconn连接异常，所以触发了这个

	if len(sclist) == 0 {
		return balancer.PickResult{}, errors.New(fmt.Sprintf("[self.%s BalancerPicker] pick failed", colorBalancerPicker))
	}
	return balancer.PickResult{
		SubConn: sclist[0],
	}, nil
}
