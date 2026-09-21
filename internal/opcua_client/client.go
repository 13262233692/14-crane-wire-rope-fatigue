// Package opcua_client 通过 OPC UA 订阅起重机 PLC 上报的起升载荷与循环次数。
package opcua_client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/monitor"
	"github.com/gopcua/opcua/ua"
)

// Config OPC UA 连接配置
type Config struct {
	Endpoint string // 例如 opc.tcp://plc.harbor:4840
	Policy   string // 安全策略, 默认 None
	Mode     string // 消息安全模式, 默认 None
	Username string // 匿名认证时留空
	Password string
	// 重连间隔
	ReconnectInterval time.Duration
}

// RopeNodes 单根钢丝绳对应的 OPC UA 节点
type RopeNodes struct {
	RopeID      string
	LoadNodeID  string // 起升载荷节点, 单位 kN, 例如 ns=2;s=Crane1.Hoist.Load
	CycleNodeID string // 累计循环次数节点, 例如 ns=2;s=Crane1.Hoist.CycleCount
}

// Reading 一次采样读数
type Reading struct {
	RopeID      string
	Timestamp   time.Time
	LoadKN      float64 // 当前起升载荷
	TotalCycles int64   // PLC 累计循环计数
}

// Client OPC UA 订阅客户端
type Client struct {
	cfg   Config
	ropes []RopeNodes
	out   chan<- Reading
}

func New(cfg Config, ropes []RopeNodes, out chan<- Reading) *Client {
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = 5 * time.Second
	}
	return &Client{cfg: cfg, ropes: ropes, out: out}
}

// Run 启动订阅循环, 断线自动重连, 直到 ctx 取消
func (c *Client) Run(ctx context.Context) error {
	for {
		err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("[opcua] 连接断开: %v, %v 后重连", err, c.cfg.ReconnectInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.cfg.ReconnectInterval):
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	opts := []opcua.Option{
		opcua.SecurityPolicy(c.cfg.Policy),
		opcua.SecurityModeString(c.cfg.Mode),
	}
	if c.cfg.Username != "" {
		opts = append(opts, opcua.AuthUsername(c.cfg.Username, c.cfg.Password))
	} else {
		opts = append(opts, opcua.AuthAnonymous())
	}

	conn, err := opcua.NewClient(c.cfg.Endpoint, opts...)
	if err != nil {
		return fmt.Errorf("new client %s: %w", c.cfg.Endpoint, err)
	}
	if err := conn.Connect(ctx); err != nil {
		return fmt.Errorf("connect %s: %w", c.cfg.Endpoint, err)
	}
	defer conn.Close(ctx)
	log.Printf("[opcua] 已连接 %s", c.cfg.Endpoint)

	m, err := monitor.NewNodeMonitor(conn)
	if err != nil {
		return fmt.Errorf("new node monitor: %w", err)
	}

	// 每根钢丝绳建立独立订阅, 载荷变化即推送
	for _, rn := range c.ropes {
		rn := rn
		sub, err := m.Subscribe(ctx, &opcua.SubscriptionParameters{
			Interval: 500 * time.Millisecond,
		}, c.makeCallback(rn), rn.LoadNodeID, rn.CycleNodeID)
		if err != nil {
			return fmt.Errorf("subscribe rope %s: %w", rn.RopeID, err)
		}
		defer sub.Unsubscribe(ctx)
		log.Printf("[opcua] 已订阅 rope=%s load=%s cycles=%s", rn.RopeID, rn.LoadNodeID, rn.CycleNodeID)
	}

	<-ctx.Done()
	return ctx.Err()
}

// makeCallback 为每根钢丝绳维护最近一次载荷/循环计数, 凑齐一对后输出 Reading
func (c *Client) makeCallback(rn RopeNodes) monitor.MsgHandler {
	var lastLoad float64
	var lastCycles int64
	var hasLoad, hasCycles bool

	return func(sub *monitor.Subscription, msg *monitor.DataChangeMessage) {
		if msg.Error != nil {
			log.Printf("[opcua] rope=%s 数据错误: %v", rn.RopeID, msg.Error)
			return
		}
		switch msg.NodeID.String() {
		case rn.LoadNodeID:
			v, err := toFloat(msg.Value.Value())
			if err != nil {
				log.Printf("[opcua] rope=%s 载荷解析失败: %v", rn.RopeID, err)
				return
			}
			lastLoad, hasLoad = v, true
		case rn.CycleNodeID:
			v, err := toInt(msg.Value.Value())
			if err != nil {
				log.Printf("[opcua] rope=%s 循环计数解析失败: %v", rn.RopeID, err)
				return
			}
			lastCycles, hasCycles = v, true
		}
		if hasLoad && hasCycles {
			select {
			case c.out <- Reading{
				RopeID:      rn.RopeID,
				Timestamp:   time.Now(),
				LoadKN:      lastLoad,
				TotalCycles: lastCycles,
			}:
			default: // 下游拥塞时丢弃, 避免阻塞订阅回调
			}
		}
	}
}

func toFloat(v interface{}) (float64, error) {
	switch t := v.(type) {
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	case int32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case uint32:
		return float64(t), nil
	case uint64:
		return float64(t), nil
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", v)
	}
}

func toInt(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case uint32:
		return int64(t), nil
	case uint64:
		return int64(t), nil
	case float32:
		return int64(t), nil
	case float64:
		return int64(t), nil
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", v)
	}
}

// 确保 ua 包被引用(预留扩展: 自定义节点浏览等)
var _ = ua.MustParseNodeID
