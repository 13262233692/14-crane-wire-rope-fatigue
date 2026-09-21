# 港口岸桥起重机钢丝绳疲劳监测微服务

基于 Miner 线性累积损伤理论的钢丝绳剩余寿命监测系统。PLC 通过 OPC UA 上报起升载荷与循环次数，服务实时计算疲劳累积损伤，超阈值时通过 gRPC 流 / SSE 推送告警，时序数据持久化到 TimescaleDB。

## 架构

```
PLC ──OPC UA──> opcua_client ──> monitor (编排)
                                    │
                    fatigue_calculator (Miner 法则: D = Σ n_i/N_i)
                                    │
                    ┌───────────────┼────────────────┐
              timeseries_dao   alert_service.Bus   (实时状态)
              (TimescaleDB)         │
                    │         ┌─────┴──────┐
              历史查询    gRPC 流订阅   HTTP SSE
```

## 模块

| 模块 | 职责 |
|---|---|
| `internal/opcua_client` | OPC UA 订阅客户端，断线自动重连，载荷/循环计数配对输出 |
| `internal/fatigue_calculator` | Miner 累积损伤计算（S-N 曲线 Basquin 方程 N=C·Δσ⁻ᵐ），阈值判定 |
| `internal/timeseries_dao` | TimescaleDB 超表读写：载荷采样、损伤快照、载荷谱聚合 |
| `internal/http_handler` | Gin REST API |
| `internal/alert_service` | 进程内告警总线 + gRPC 流式订阅服务 |
| `internal/monitor` | 编排核心：读数差分 → 疲劳计算 → 持久化 → 告警 |

## API

**REST（默认 :8080）**

- `GET /api/v1/ropes/:id/life` — 剩余寿命查询（Miner 损伤 D、剩余寿命%、预计总寿命循环数、告警级别）
- `GET /api/v1/ropes/:id/load-spectrum?from=&to=&bucket_kn=5` — 历史载荷谱直方图（载荷区间 → 循环数聚合）
- `GET /api/v1/ropes/:id/damage-history` — 损伤历史趋势
- `GET /api/v1/alerts/subscribe?rope_ids=rope-1&min_level=2` — 告警订阅（SSE 长连接）

**gRPC（默认 :9090）**

- `AlertService.SubscribeAlerts(SubscribeRequest) returns (stream Alert)` — 告警流订阅，proto 见 `proto/alert/v1/alert.proto`

## 运行

```bash
# 1. 启动 TimescaleDB
docker compose -f deployments/docker-compose.yml up -d

# 2. 配置并启动服务
export OPCUA_ENDPOINT=opc.tcp://plc.harbor:4840
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/crane_fatigue?sslmode=disable
go run ./cmd/server

# 3. 查询示例
curl localhost:8080/api/v1/ropes/rope-1/life
curl "localhost:8080/api/v1/ropes/rope-1/load-spectrum?bucket_kn=10"
curl -N "localhost:8080/api/v1/alerts/subscribe?min_level=2"
```

## 测试

```bash
go test ./...
```

## 说明

- S-N 曲线参数（`DefaultWireRopeSN`: C=1e15, m=3.5）为工程估算值，实际部署应按钢丝绳厂家试验数据标定。
- PLC 上报累计循环计数，服务侧做差分得到增量；计数复位（换绳/PLC 重启）自动处理。
- 告警级别跃升（NONE→WARNING→CRITICAL）时触发一次告警，避免重复推送。
