// 港口岸桥起重机钢丝绳疲劳监测微服务
//
// 数据流: PLC --(OPC UA)--> 疲劳计算(Miner) --+--> TimescaleDB
//
//	+--> 告警总线 --(gRPC stream / SSE)--> 客户端
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"

	"crane-fatigue-monitor/internal/alert_service"
	"crane-fatigue-monitor/internal/fatigue_calculator"
	"crane-fatigue-monitor/internal/http_handler"
	"crane-fatigue-monitor/internal/monitor"
	"crane-fatigue-monitor/internal/opcua_client"
	"crane-fatigue-monitor/internal/timeseries_dao"
	alertv1 "crane-fatigue-monitor/proto/alert/v1"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---- 配置(环境变量) ----
	opcEndpoint := env("OPCUA_ENDPOINT", "opc.tcp://127.0.0.1:4840")
	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:5432/crane_fatigue?sslmode=disable")
	httpAddr := env("HTTP_ADDR", ":8080")
	grpcAddr := env("GRPC_ADDR", ":9090")

	// ---- 组件初始化 ----
	dao, err := timeseries_dao.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("初始化 TimescaleDB 失败: %v", err)
	}
	defer dao.Close()

	bus := alert_service.NewBus()

	ropes := []monitor.RopeConfig{
		{RopeID: "rope-1", RopeAreaMM2: 415.0}, // 示例: Ø32mm 6x36WS 金属截面积
		{RopeID: "rope-2", RopeAreaMM2: 415.0},
	}
	svc := monitor.New(dao, bus, ropes, fatigue_calculator.DefaultThresholds)

	// ---- OPC UA 订阅 ----
	readings := make(chan opcua_client.Reading, 256)
	opcClient := opcua_client.New(
		opcua_client.Config{Endpoint: opcEndpoint, Policy: "None", Mode: "None"},
		[]opcua_client.RopeNodes{
			{RopeID: "rope-1", LoadNodeID: "ns=2;s=QC1.Hoist.Load", CycleNodeID: "ns=2;s=QC1.Hoist.CycleCount"},
			{RopeID: "rope-2", LoadNodeID: "ns=2;s=QC2.Hoist.Load", CycleNodeID: "ns=2;s=QC2.Hoist.CycleCount"},
		},
		readings,
	)
	go func() {
		if err := opcClient.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("OPC UA 客户端退出: %v", err)
		}
	}()
	go svc.Consume(ctx, readings)

	// ---- gRPC 告警订阅服务 ----
	grpcSrv := grpc.NewServer()
	alertv1.RegisterAlertServiceServer(grpcSrv, alert_service.NewServer(bus))
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("gRPC 监听失败: %v", err)
	}
	go func() {
		log.Printf("gRPC 告警服务监听 %s", grpcAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("gRPC 服务退出: %v", err)
		}
	}()

	// ---- HTTP REST API ----
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	http_handler.New(svc, dao, bus).RegisterRoutes(r)
	httpSrv := &http.Server{Addr: httpAddr, Handler: r}
	go func() {
		log.Printf("HTTP API 监听 %s", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP 服务退出: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("正在关闭...")

	grpcSrv.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
