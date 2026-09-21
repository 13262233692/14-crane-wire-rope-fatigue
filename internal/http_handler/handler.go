// Package http_handler 提供 REST API: 寿命查询 / 历史载荷谱 / 告警订阅(SSE)。
package http_handler

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"crane-fatigue-monitor/internal/alert_service"
	"crane-fatigue-monitor/internal/fatigue_calculator"
	"crane-fatigue-monitor/internal/timeseries_dao"
)

// LifeProvider 从监控核心获取实时寿命状态
type LifeProvider interface {
	State(ropeID string) (fatigue_calculator.DamageState, bool)
	RopeIDs() []string
}

// Handler REST API 处理器
type Handler struct {
	monitor LifeProvider
	dao     *timeseries_dao.DAO
	bus     *alert_service.Bus
}

func New(m LifeProvider, dao *timeseries_dao.DAO, bus *alert_service.Bus) *Handler {
	return &Handler{monitor: m, dao: dao, bus: bus}
}

// RegisterRoutes 注册路由
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	v1 := r.Group("/api/v1")
	{
		v1.GET("/ropes", h.ListRopes)
		v1.GET("/ropes/:id/life", h.GetRemainingLife)
		v1.GET("/ropes/:id/load-spectrum", h.GetLoadSpectrum)
		v1.GET("/ropes/:id/damage-history", h.GetDamageHistory)
		v1.GET("/alerts/subscribe", h.SubscribeAlertsSSE)
	}
}

// ListRopes GET /api/v1/ropes
func (h *Handler) ListRopes(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"rope_ids": h.monitor.RopeIDs()})
}

// GetRemainingLife GET /api/v1/ropes/:id/life
// 返回实时 Miner 累积损伤与剩余寿命
func (h *Handler) GetRemainingLife(c *gin.Context) {
	ropeID := c.Param("id")
	state, ok := h.monitor.State(ropeID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("rope %s not found", ropeID)})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"rope_id":            ropeID,
		"damage_fraction":    state.DamageFraction,
		"remaining_life_pct": state.RemainingLife,
		"total_cycles":       state.TotalCycles,
		"estimated_cycles":   state.EstimatedCycles,
		"alert_level":        state.Level.String(),
		"updated_at":         state.UpdatedAt,
	})
}

// GetLoadSpectrum GET /api/v1/ropes/:id/load-spectrum?from=...&to=...&bucket_kn=5
// 返回历史载荷谱直方图(载荷区间 -> 循环数)
func (h *Handler) GetLoadSpectrum(c *gin.Context) {
	ropeID := c.Param("id")
	from, to := parseTimeRange(c, 24*time.Hour)
	bucket, _ := strconv.ParseFloat(c.DefaultQuery("bucket_kn", "5"), 64)

	buckets, err := h.dao.LoadSpectrum(c.Request.Context(), ropeID, from, to, bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"rope_id":   ropeID,
		"from":      from,
		"to":        to,
		"bucket_kn": bucket,
		"spectrum":  buckets,
	})
}

// GetDamageHistory GET /api/v1/ropes/:id/damage-history
func (h *Handler) GetDamageHistory(c *gin.Context) {
	ropeID := c.Param("id")
	from, to := parseTimeRange(c, 7*24*time.Hour)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "1000"))

	hist, err := h.dao.DamageHistory(c.Request.Context(), ropeID, from, to, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rope_id": ropeID, "history": hist})
}

// SubscribeAlertsSSE GET /api/v1/alerts/subscribe?rope_ids=a,b&min_level=2
// REST 侧告警订阅: Server-Sent Events 长连接推送
func (h *Handler) SubscribeAlertsSSE(c *gin.Context) {
	var ropeIDs []string
	if q := c.Query("rope_ids"); q != "" {
		for _, id := range splitComma(q) {
			ropeIDs = append(ropeIDs, id)
		}
	}
	minLevel, _ := strconv.Atoi(c.DefaultQuery("min_level", "2"))

	ch, cancel := h.bus.Subscribe(ropeIDs, int32(minLevel))
	defer cancel()

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")

	ctx := c.Request.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case a, ok := <-ch:
			if !ok {
				return
			}
			c.SSEvent("alert", a)
			c.Writer.Flush()
		}
	}
}

func parseTimeRange(c *gin.Context, defaultSpan time.Duration) (time.Time, time.Time) {
	to := time.Now()
	from := to.Add(-defaultSpan)
	if v := c.Query("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = t
		}
	}
	if v := c.Query("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			to = t
		}
	}
	return from, to
}

func splitComma(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
