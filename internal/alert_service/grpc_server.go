// Package alert_service 提供疲劳告警的事件总线与 gRPC 流式订阅服务。
package alert_service

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	alertv1 "crane-fatigue-monitor/proto/alert/v1"
)

// Event 内部告警事件
type Event struct {
	RopeID         string
	Level          int32
	Message        string
	DamageFraction float64
	RemainingLife  float64
}

type subscription struct {
	ch         chan *alertv1.Alert
	ropeFilter map[string]bool
	minLevel   int32
}

// Bus 进程内告警总线: 疲劳计算模块发布, gRPC/HTTP 订阅端消费
type Bus struct {
	mu   sync.RWMutex
	subs map[chan *alertv1.Alert]subscription
}

func NewBus() *Bus {
	return &Bus{subs: make(map[chan *alertv1.Alert]subscription)}
}

// Publish 发布告警事件
func (b *Bus) Publish(e Event) {
	a := &alertv1.Alert{
		AlertId:          uuid.NewString(),
		RopeId:           e.RopeID,
		Level:            e.Level,
		Message:          e.Message,
		DamageFraction:   e.DamageFraction,
		RemainingLifePct: e.RemainingLife,
		OccurredAtUnix:   time.Now().Unix(),
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch, sub := range b.subs {
		if sub.minLevel > 0 && a.Level < sub.minLevel {
			continue
		}
		if len(sub.ropeFilter) > 0 && !sub.ropeFilter[a.RopeId] {
			continue
		}
		select {
		case ch <- a:
		default: // 订阅方消费过慢时丢弃, 防止阻塞发布方
		}
	}
}

// Subscribe 注册订阅通道, 返回取消函数 (供 HTTP SSE 与 gRPC 复用)
func (b *Bus) Subscribe(ropeIDs []string, minLevel int32) (<-chan *alertv1.Alert, func()) {
	ch := make(chan *alertv1.Alert, 64)
	filter := make(map[string]bool, len(ropeIDs))
	for _, id := range ropeIDs {
		filter[id] = true
	}
	b.mu.Lock()
	b.subs[ch] = subscription{ch: ch, ropeFilter: filter, minLevel: minLevel}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
		close(ch)
	}
}

// Server gRPC AlertService 实现
type Server struct {
	alertv1.UnimplementedAlertServiceServer
	bus *Bus
}

func NewServer(bus *Bus) *Server { return &Server{bus: bus} }

// SubscribeAlerts 流式推送告警, 客户端断开时自动清理
func (s *Server) SubscribeAlerts(req *alertv1.SubscribeRequest, stream alertv1.AlertService_SubscribeAlertsServer) error {
	ch, cancel := s.bus.Subscribe(req.RopeIds, req.MinLevel)
	defer cancel()
	for {
		select {
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		case a, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(a); err != nil {
				return status.Errorf(codes.Internal, "send alert: %v", err)
			}
		}
	}
}
