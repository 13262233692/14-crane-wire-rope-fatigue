// Package timeseries_dao 封装 TimescaleDB 时序数据存取:
// 载荷谱(load_samples)与损伤快照(damage_snapshots)两张超表。
package timeseries_dao

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadSample 载荷谱采样点
type LoadSample struct {
	Time   time.Time `json:"time"`
	RopeID string    `json:"rope_id"`
	LoadKN float64   `json:"load_kn"`
	Cycles int64     `json:"cycles"`
}

// DamageSnapshot 疲劳损伤快照
type DamageSnapshot struct {
	Time            time.Time `json:"time"`
	RopeID          string    `json:"rope_id"`
	DamageFraction  float64   `json:"damage_fraction"`
	TotalCycles     int64     `json:"total_cycles"`
	RemainingLife   float64   `json:"remaining_life_pct"`
	EstimatedCycles float64   `json:"estimated_cycles"`
}

// LoadSpectrumBucket 载荷谱直方图桶(按载荷区间聚合循环数)
type LoadSpectrumBucket struct {
	LoadMinKN float64 `json:"load_min_kn"`
	LoadMaxKN float64 `json:"load_max_kn"`
	Cycles    int64   `json:"cycles"`
	Samples   int64   `json:"samples"`
}

// DAO 数据访问对象
type DAO struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, connString string) (*DAO, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("connect timescaledb: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping timescaledb: %w", err)
	}
	return &DAO{pool: pool}, nil
}

func (d *DAO) Close() { d.pool.Close() }

// InsertLoadSample 写入载荷采样
func (d *DAO) InsertLoadSample(ctx context.Context, s LoadSample) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO load_samples (time, rope_id, load_kn, cycles) VALUES ($1,$2,$3,$4)`,
		s.Time, s.RopeID, s.LoadKN, s.Cycles)
	return err
}

// InsertDamageSnapshot 写入损伤快照
func (d *DAO) InsertDamageSnapshot(ctx context.Context, s DamageSnapshot) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO damage_snapshots (time, rope_id, damage_fraction, total_cycles, remaining_life_pct, estimated_cycles)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		s.Time, s.RopeID, s.DamageFraction, s.TotalCycles, s.RemainingLife, s.EstimatedCycles)
	return err
}

// LatestDamage 查询钢丝绳最新损伤状态
func (d *DAO) LatestDamage(ctx context.Context, ropeID string) (*DamageSnapshot, error) {
	var s DamageSnapshot
	err := d.pool.QueryRow(ctx,
		`SELECT time, rope_id, damage_fraction, total_cycles, remaining_life_pct, estimated_cycles
		 FROM damage_snapshots WHERE rope_id=$1 ORDER BY time DESC LIMIT 1`, ropeID).
		Scan(&s.Time, &s.RopeID, &s.DamageFraction, &s.TotalCycles, &s.RemainingLife, &s.EstimatedCycles)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// LoadSpectrum 按载荷区间聚合历史载荷谱直方图
func (d *DAO) LoadSpectrum(ctx context.Context, ropeID string, from, to time.Time, bucketWidthKN float64) ([]LoadSpectrumBucket, error) {
	if bucketWidthKN <= 0 {
		bucketWidthKN = 5.0
	}
	rows, err := d.pool.Query(ctx,
		`SELECT floor(load_kn / $4) * $4            AS load_min,
		        floor(load_kn / $4) * $4 + $4       AS load_max,
		        SUM(cycles)                         AS cycles,
		        COUNT(*)                            AS samples
		 FROM load_samples
		 WHERE rope_id = $1 AND time >= $2 AND time < $3
		 GROUP BY load_min
		 ORDER BY load_min`,
		ropeID, from, to, bucketWidthKN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LoadSpectrumBucket
	for rows.Next() {
		var b LoadSpectrumBucket
		if err := rows.Scan(&b.LoadMinKN, &b.LoadMaxKN, &b.Cycles, &b.Samples); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// DamageHistory 查询损伤历史(用于趋势图)
func (d *DAO) DamageHistory(ctx context.Context, ropeID string, from, to time.Time, limit int) ([]DamageSnapshot, error) {
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	rows, err := d.pool.Query(ctx,
		`SELECT time, rope_id, damage_fraction, total_cycles, remaining_life_pct, estimated_cycles
		 FROM damage_snapshots
		 WHERE rope_id=$1 AND time >= $2 AND time < $3
		 ORDER BY time DESC LIMIT $4`, ropeID, from, to, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DamageSnapshot
	for rows.Next() {
		var s DamageSnapshot
		if err := rows.Scan(&s.Time, &s.RopeID, &s.DamageFraction, &s.TotalCycles, &s.RemainingLife, &s.EstimatedCycles); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
