// Package monitor 编排数据流: OPC UA 读数 -> Miner 疲劳计算 -> TimescaleDB 持久化 -> 告警总线。
package monitor

import (
	"context"
	"log"
	"sort"
	"sync"

	"crane-fatigue-monitor/internal/alert_service"
	"crane-fatigue-monitor/internal/fatigue_calculator"
	"crane-fatigue-monitor/internal/opcua_client"
	"crane-fatigue-monitor/internal/timeseries_dao"
)

// RopeConfig 单根钢丝绳配置
type RopeConfig struct {
	RopeID      string
	RopeAreaMM2 float64 // 金属截面积 mm²
}

// Service 监控核心服务
type Service struct {
	dao    *timeseries_dao.DAO
	bus    *alert_service.Bus
	thresh fatigue_calculator.Thresholds

	mu    sync.RWMutex
	calcs map[string]*fatigue_calculator.Calculator
	// 每根钢丝绳最近一次处理的 PLC 累计循环数, 用于差分
	lastCycles map[string]int64
	// 已发布的最高告警级别, 避免重复告警
	lastAlertLevel map[string]fatigue_calculator.AlertLevel
}

func New(dao *timeseries_dao.DAO, bus *alert_service.Bus, ropes []RopeConfig, thresh fatigue_calculator.Thresholds) *Service {
	s := &Service{
		dao:            dao,
		bus:            bus,
		thresh:         thresh,
		calcs:          make(map[string]*fatigue_calculator.Calculator),
		lastCycles:     make(map[string]int64),
		lastAlertLevel: make(map[string]fatigue_calculator.AlertLevel),
	}
	for _, r := range ropes {
		s.calcs[r.RopeID] = fatigue_calculator.NewCalculator(r.RopeAreaMM2, fatigue_calculator.DefaultWireRopeSN, thresh)
	}
	return s
}

// State 返回钢丝绳实时寿命状态 (实现 http_handler.LifeProvider)
func (s *Service) State(ropeID string) (fatigue_calculator.DamageState, bool) {
	s.mu.RLock()
	calc, ok := s.calcs[ropeID]
	s.mu.RUnlock()
	if !ok {
		return fatigue_calculator.DamageState{}, false
	}
	return calc.State(), true
}

// RopeIDs 返回全部钢丝绳 ID
func (s *Service) RopeIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.calcs))
	for id := range s.calcs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Consume 消费 OPC UA 读数流, 直到 ctx 取消
func (s *Service) Consume(ctx context.Context, readings <-chan opcua_client.Reading) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-readings:
			s.process(ctx, r)
		}
	}
}

func (s *Service) process(ctx context.Context, r opcua_client.Reading) {
	s.mu.RLock()
	calc, ok := s.calcs[r.RopeID]
	s.mu.RUnlock()
	if !ok {
		return
	}

	// PLC 上报的是累计循环数, 差分得到本批次增量
	s.mu.Lock()
	prev, seen := s.lastCycles[r.RopeID]
	var delta int64
	switch {
	case !seen:
		delta = 0 // 首次采样仅建立基线
	case r.TotalCycles >= prev:
		delta = r.TotalCycles - prev
	default:
		delta = r.TotalCycles // PLC 计数复位(换绳/重启), 从新值起算
	}
	s.lastCycles[r.RopeID] = r.TotalCycles
	s.mu.Unlock()

	if delta <= 0 {
		return
	}

	state := calc.AddCycles(fatigue_calculator.CycleSample{
		Timestamp: r.Timestamp,
		LoadKN:    r.LoadKN,
		Cycles:    delta,
	})

	// 持久化时序数据(异步批量亦可, 此处逐条写入保持简单)
	if err := s.dao.InsertLoadSample(ctx, timeseries_dao.LoadSample{
		Time: r.Timestamp, RopeID: r.RopeID, LoadKN: r.LoadKN, Cycles: delta,
	}); err != nil {
		log.Printf("[monitor] 写入载荷采样失败: %v", err)
	}
	if err := s.dao.InsertDamageSnapshot(ctx, timeseries_dao.DamageSnapshot{
		Time: r.Timestamp, RopeID: r.RopeID,
		DamageFraction: state.DamageFraction, TotalCycles: state.TotalCycles,
		RemainingLife: state.RemainingLife, EstimatedCycles: state.EstimatedCycles,
	}); err != nil {
		log.Printf("[monitor] 写入损伤快照失败: %v", err)
	}

	s.maybeAlert(r.RopeID, state)
}

// maybeAlert 告警级别跃升时发布告警
func (s *Service) maybeAlert(ropeID string, state fatigue_calculator.DamageState) {
	s.mu.Lock()
	prev := s.lastAlertLevel[ropeID]
	if state.Level <= prev {
		s.mu.Unlock()
		return
	}
	s.lastAlertLevel[ropeID] = state.Level
	s.mu.Unlock()

	var level int32
	var msg string
	switch state.Level {
	case fatigue_calculator.LevelCritical:
		level = 3
		msg = "钢丝绳疲劳损伤超过临界阈值, 建议立即停机更换"
	case fatigue_calculator.LevelWarning:
		level = 2
		msg = "钢丝绳疲劳损伤超过预警阈值, 请安排检修计划"
	default:
		return
	}
	log.Printf("[alert] rope=%s level=%s D=%.4f remaining=%.1f%%", ropeID, state.Level, state.DamageFraction, state.RemainingLife)
	s.bus.Publish(alert_service.Event{
		RopeID:         ropeID,
		Level:          level,
		Message:        msg,
		DamageFraction: state.DamageFraction,
		RemainingLife:  state.RemainingLife,
	})
}

// ResetRope 换绳后重置(可扩展为管理 API)
func (s *Service) ResetRope(ropeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if calc, ok := s.calcs[ropeID]; ok {
		calc.Reset()
	}
	s.lastCycles[ropeID] = 0
	s.lastAlertLevel[ropeID] = fatigue_calculator.LevelNone
}
