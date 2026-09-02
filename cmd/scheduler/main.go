package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	schedulerv1 "github.com/yowaimono/go-grpc-scheduler/api/gen"
	"github.com/yowaimono/go-grpc-scheduler/internal/auth"
	"github.com/yowaimono/go-grpc-scheduler/internal/observability"
	"github.com/yowaimono/go-grpc-scheduler/internal/scheduler"
	"github.com/yowaimono/go-grpc-scheduler/internal/storage/postgres"
	"github.com/yowaimono/go-grpc-scheduler/internal/transport/grpcserver"
	"github.com/yowaimono/go-grpc-scheduler/internal/transport/httpapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	shutdownTracing, err := observability.SetupTracing(context.Background())
	if err == nil {
		defer shutdownTracing(context.Background())
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeID := getenv("SCHEDULER_ID", "scheduler-local")
	httpAddr := getenv("HTTP_ADDR", ":8080")
	grpcAddr := getenv("GRPC_ADDR", ":9090")
	coord := &scheduler.Coordinator{}
	metrics := observability.NewMetrics()
	engine := scheduler.New(nodeID, coord).WithMetrics(metrics)
	engine.SetRole(scheduler.RoleMaster)
	roleName := "master"

	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		store, err := postgres.Open(ctx, dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer store.Close()
		if err := store.Migrate(ctx); err != nil {
			log.Fatal(err)
		}
		engine.WithRepository(store)
		_ = engine.Sync(ctx, time.Now().Add(-24*time.Hour))
		acquired, epoch, err := store.TryAcquireLeader(ctx, getenv("SCHEDULER_CLUSTER", "default"), nodeID, 5*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		if !acquired {
			engine.SetRole(scheduler.RoleSlave)
			roleName = "slave"
		} else {
			engine.SetEpoch(epoch)
		}
		go electionLoop(ctx, store, engine, getenv("SCHEDULER_CLUSTER", "default"), nodeID, epoch)
		go syncLoop(ctx, store, engine)
	}

	workerAuth := auth.FromEnv("SCHEDULER_WORKER_TOKENS")
	grpcOptions := []grpc.ServerOption{}
	if certFile, keyFile := os.Getenv("GRPC_CERT_FILE"), os.Getenv("GRPC_KEY_FILE"); certFile != "" && keyFile != "" {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}
		tlsConfig := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
		if caFile := os.Getenv("GRPC_CA_FILE"); caFile != "" {
			pem, err := os.ReadFile(caFile)
			if err != nil {
				log.Fatal(err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				log.Fatal("invalid GRPC_CA_FILE")
			}
			tlsConfig.ClientCAs = pool
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
		grpcOptions = append(grpcOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	grpcSrv := grpc.NewServer(grpcOptions...)
	schedulerv1.RegisterSchedulerServer(grpcSrv, &grpcserver.Server{Engine: engine, WorkerAuth: workerAuth})
	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Printf("gRPC listening on %s", grpcAddr)
		if err := grpcSrv.Serve(listener); err != nil {
			log.Printf("gRPC stopped: %v", err)
		}
	}()

	apiSrv := httpapi.New(engine, nodeID, roleName, metrics)
	apiSrv.Auth = auth.FromEnv("SCHEDULER_API_TOKENS")
	if origins := os.Getenv("SCHEDULER_ALLOWED_ORIGINS"); origins != "" {
		apiSrv.AllowedOrigins = map[string]struct{}{}
		for _, origin := range strings.Split(origins, ",") {
			apiSrv.AllowedOrigins[strings.TrimSpace(origin)] = struct{}{}
		}
	}
	if raw := os.Getenv("SCHEDULER_MAX_PAYLOAD_BYTES"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			apiSrv.MaxPayloadBytes = n
		}
	}
	if raw := os.Getenv("SCHEDULER_MAX_TASK_TIMEOUT_MS"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			apiSrv.MaxTaskTimeout = time.Duration(n) * time.Millisecond
		}
	}
	server := &http.Server{Addr: httpAddr, Handler: apiSrv.Handler()}
	go func() {
		log.Printf("HTTP listening on %s", httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	stopTicker := make(chan struct{})
	go httpapi.RunTicker(stopTicker, engine)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	engine.BeginDrain()
	close(stopTicker)
	grpcSrv.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
}

func electionLoop(ctx context.Context, store *postgres.Store, engine *scheduler.Engine, clusterID, nodeID string, initialEpoch int64) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	epoch := initialEpoch
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if engine.Role() == scheduler.RoleMaster {
				ok, err := store.HeartbeatLeader(ctx, clusterID, nodeID, epoch, 5*time.Second)
				if err != nil || !ok {
					engine.SetRole(scheduler.RoleSlave)
					continue
				}
			} else {
				ok, next, err := store.TryAcquireLeader(ctx, clusterID, nodeID, 5*time.Second)
				if err == nil && ok {
					epoch = next
					engine.SetEpoch(epoch)
					engine.SetRole(scheduler.RoleMaster)
				}
			}
		}
	}
}

func syncLoop(ctx context.Context, store *postgres.Store, engine *scheduler.Engine) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = engine.Sync(ctx, time.Now().Add(-2*time.Minute))
		}
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
