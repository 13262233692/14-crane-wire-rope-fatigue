package fatigue_calculator

import (
	"math"
	"testing"
	"time"
)

func newTestCalc() *Calculator {
	return NewCalculator(100.0, SNCurve{C: 1.0e12, M: 3.0}, DefaultThresholds)
}

func TestCyclesToFailure(t *testing.T) {
	c := newTestCalc()
	// Δσ = 100 MPa -> N = 1e12 / 100^3 = 1e6
	if got := c.CyclesToFailure(100); math.Abs(got-1e6) > 1 {
		t.Fatalf("CyclesToFailure(100) = %v, want 1e6", got)
	}
	if got := c.CyclesToFailure(0); !math.IsInf(got, 1) {
		t.Fatalf("CyclesToFailure(0) should be +Inf, got %v", got)
	}
}

func TestMinerAccumulation(t *testing.T) {
	c := newTestCalc()
	// 10 kN / 100 mm² = 100 MPa -> N = 1e12/100^3 = 1e6
	st := c.AddCycles(CycleSample{Timestamp: time.Now(), LoadKN: 10, Cycles: 500_000})
	want := 0.5
	if math.Abs(st.DamageFraction-want) > 1e-12 {
		t.Fatalf("damage = %v, want %v", st.DamageFraction, want)
	}
	if st.Level != LevelNone {
		t.Fatalf("level = %v, want NONE", st.Level)
	}
}

func TestThresholdAlerts(t *testing.T) {
	c := newTestCalc()
	var st DamageState
	// 10 kN -> Δσ=100MPa -> N=1e6; 施加 8e5 循环 -> D=0.8
	st = c.AddCycles(CycleSample{Timestamp: time.Now(), LoadKN: 10, Cycles: 800_000})
	if st.Level != LevelWarning {
		t.Fatalf("level = %v, want WARNING (D=%.2f)", st.Level, st.DamageFraction)
	}
	st = c.AddCycles(CycleSample{Timestamp: time.Now(), LoadKN: 10, Cycles: 200_000})
	if st.Level != LevelCritical {
		t.Fatalf("level = %v, want CRITICAL (D=%.2f)", st.Level, st.DamageFraction)
	}
	if st.RemainingLife > 0.001 {
		t.Fatalf("remaining life should be ~0, got %v", st.RemainingLife)
	}
}

func TestReset(t *testing.T) {
	c := newTestCalc()
	c.AddCycles(CycleSample{Timestamp: time.Now(), LoadKN: 10, Cycles: 1e5})
	c.Reset()
	st := c.State()
	if st.DamageFraction != 0 || st.TotalCycles != 0 {
		t.Fatalf("after reset: D=%v cycles=%v, want 0/0", st.DamageFraction, st.TotalCycles)
	}
}
