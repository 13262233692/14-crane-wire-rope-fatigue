// Package fatigue_calculator 基于 Miner 线性累积损伤理论计算钢丝绳疲劳寿命。
//
// Miner 法则: D = Σ (n_i / N_i)
//   - n_i: 应力幅 Δσ_i 下的实际循环次数
//   - N_i: 应力幅 Δσ_i 下由 S-N 曲线得到的失效循环数, N = C / Δσ^m
//
// 当 D >= 1.0 时认为钢丝绳达到疲劳寿命极限。
package fatigue_calculator

import (
	"math"
	"sync"
	"time"
)

// SNCurve S-N 曲线参数 (Basquin 方程: N = C * Δσ^(-m))
type SNCurve struct {
	C float64 // 材料常数
	M float64 // S-N 曲线斜率指数, 钢丝绳典型值 3.0~4.0
}

// 典型 6×36 类钢丝绳在空气环境中的近似 S-N 参数(工程估算值, 实际应由试验标定)
var DefaultWireRopeSN = SNCurve{C: 1.0e15, M: 3.5}

// AlertLevel 告警级别
type AlertLevel int

const (
	LevelNone AlertLevel = iota
	LevelInfo
	LevelWarning
	LevelCritical
)

func (l AlertLevel) String() string {
	switch l {
	case LevelInfo:
		return "INFO"
	case LevelWarning:
		return "WARNING"
	case LevelCritical:
		return "CRITICAL"
	default:
		return "NONE"
	}
}

// Thresholds 损伤告警阈值
type Thresholds struct {
	Warning  float64 // 默认 0.7
	Critical float64 // 默认 0.9
}

var DefaultThresholds = Thresholds{Warning: 0.7, Critical: 0.9}

// CycleSample 一次起升循环样本
type CycleSample struct {
	Timestamp time.Time
	LoadKN    float64 // 起升载荷 kN
	Cycles    int64   // 该样本对应的循环次数
}

// DamageState 累积损伤状态快照
type DamageState struct {
	DamageFraction  float64    // Miner 累积损伤 D
	TotalCycles     int64      // 累计循环次数
	RemainingLife   float64    // 剩余寿命百分比 0~100
	EstimatedCycles float64    // 按当前载荷谱估计的总寿命循环数
	Level           AlertLevel // 当前告警级别
	UpdatedAt       time.Time
}

// Calculator 单根钢丝绳的疲劳计算器 (线程安全)
type Calculator struct {
	mu     sync.RWMutex
	sn     SNCurve
	thresh Thresholds

	// ropeArea 钢丝绳金属截面积 mm², 用于将载荷换算为应力幅
	ropeArea float64

	damage      float64
	totalCycles int64
	updatedAt   time.Time
}

// NewCalculator 创建疲劳计算器
// ropeAreaMM2: 钢丝绳金属截面积; sn: S-N 曲线参数; thresh: 告警阈值
func NewCalculator(ropeAreaMM2 float64, sn SNCurve, thresh Thresholds) *Calculator {
	return &Calculator{
		sn:       sn,
		thresh:   thresh,
		ropeArea: ropeAreaMM2,
	}
}

// stressRangeMPa 将起升载荷换算为钢丝绳应力幅 (MPa = N/mm²)
// 简化模型: Δσ = F / A, 1 kN / 1 mm² = 1 MPa
func (c *Calculator) stressRangeMPa(loadKN float64) float64 {
	if c.ropeArea <= 0 {
		return 0
	}
	return loadKN * 1000.0 / c.ropeArea // kN -> N, N/mm² = MPa
}

// CyclesToFailure 由 S-N 曲线计算给定应力幅下的失效循环数
func (c *Calculator) CyclesToFailure(stressRangeMPa float64) float64 {
	if stressRangeMPa <= 0 {
		return math.Inf(1)
	}
	return c.sn.C / math.Pow(stressRangeMPa, c.sn.M)
}

// AddCycles 累加一批循环并返回最新损伤状态
func (c *Calculator) AddCycles(s CycleSample) DamageState {
	c.mu.Lock()
	defer c.mu.Unlock()

	stress := c.stressRangeMPa(s.LoadKN)
	nf := c.CyclesToFailure(stress)
	if !math.IsInf(nf, 1) && nf > 0 {
		c.damage += float64(s.Cycles) / nf
	}
	c.totalCycles += s.Cycles
	c.updatedAt = s.Timestamp

	return c.stateLocked()
}

// State 返回当前损伤状态快照
func (c *Calculator) State() DamageState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stateLocked()
}

func (c *Calculator) stateLocked() DamageState {
	remaining := (1.0 - c.damage) * 100.0
	if remaining < 0 {
		remaining = 0
	}
	var estCycles float64
	if c.damage > 0 {
		estCycles = float64(c.totalCycles) / c.damage
	}
	return DamageState{
		DamageFraction:  c.damage,
		TotalCycles:     c.totalCycles,
		RemainingLife:   remaining,
		EstimatedCycles: estCycles,
		Level:           c.levelLocked(),
		UpdatedAt:       c.updatedAt,
	}
}

func (c *Calculator) levelLocked() AlertLevel {
	switch {
	case c.damage >= c.thresh.Critical:
		return LevelCritical
	case c.damage >= c.thresh.Warning:
		return LevelWarning
	default:
		return LevelNone
	}
}

// Reset 更换钢丝绳后重置累积损伤
func (c *Calculator) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.damage = 0
	c.totalCycles = 0
	c.updatedAt = time.Now()
}
