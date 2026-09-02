# go-grpc-scheduler

一个面向高吞吐任务分发的 Go 调度器骨架，目标是支持：

- gRPC 作为控制面与数据面的统一协议
- 多 worker 并行执行
- 多调度器（master/slave）部署
- master 只负责调度决策，slave 可水平扩展并在故障时接管
- 租约（lease）保证 worker 崩溃后任务可以重新投递

## 架构想法

### 1. 控制面与执行面分离

调度器不执行用户任务，只做任务状态机、worker 选择和租约管理。worker 通过长连接注册、领取任务、续租并回报结果。这样调度器可以保持轻量，任务执行耗时不会阻塞调度循环。

### 2. master/slave 不共享调度状态

所有调度写入都经过一个可替换的 `Coordinator`。当前仓库提供内存实现用于本地开发；生产实现应替换为 etcd/Consul 或带 fencing token 的数据库。master 持有 lease/fencing token，slave 只读并准备接管，避免双主写入。

### 3. worker 拉取 + 租约

worker 主动拉取任务比调度器向 worker 推送更容易做背压。每个任务带 `lease_until`，worker 定期 heartbeat 续租；租约过期后任务回到 ready 队列。任务处理器必须幂等，结果提交使用任务版本号防止旧 worker 覆盖新结果。

### 4. 性能路径

- 调度热路径使用 channel + heap，避免每次分配锁竞争
- worker 注册信息按能力标签分桶，减少筛选范围
- gRPC 使用长连接和流式 heartbeat，批量领取任务
- 生产环境可把状态存储和队列替换为 Redis Streams / NATS JetStream

## 本地运行

```powershell
go test ./...
go run ./cmd/scheduler
```

覆盖率文件必须输出到仓库外，避免 Go 将根目录下的输出文件误识别为包：

```powershell
$coverageFile = Join-Path $env:TEMP "go-grpc-scheduler-cover.out"
go test "-coverprofile=$coverageFile" ./...
go tool cover "-func=$coverageFile"
```

`cmd/scheduler` 启动 HTTP API（默认 `:8080`）和 Worker gRPC（默认 `:9090`）。未配置 `DATABASE_URL` 时使用本地内存选主；配置 PostgreSQL 后启用 SQL lease/epoch 选主与任务持久化。`api/scheduler.proto` 是协议源文件，`api/gen` 为正式生成的 protobuf/gRPC stub。

## Worker SDK

业务服务可以直接通过 Go module 引入 SDK：

```go
tasksdk.Worker().
    Config(&tasksdk.Config{WorkerID: "worker-01", Roles: []string{"default"}, Slots: 4, SchedulerInstances: []string{"localhost:9090"}}).
    Register(tasksdk.DefineTask("demo.echo", "default", tasks.Echo{})).
    Start(ctx)
```

任务通过 `task.Context` 获取自定义参数，并继承标准 Go Context 的 timeout/cancel 语义。调度器时间轮采用 100ms tick、1 小时覆盖窗口，1 秒周期任务实测抖动约 40ms（受机器负载影响）。

本地演示可启动一个真实 SDK Worker：

```powershell
go run ./cmd/demo-worker
```

它注册 `demo.echo` 到 `default` role。管理台创建该任务后，Worker 会持续轮询空闲队列并回报执行结果。

## PostgreSQL

```powershell
docker compose up -d postgres
$env:DATABASE_URL = "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"
go run ./cmd/scheduler
```

## 管理前端

```powershell
cd web
npm install
$env:VITE_SCHEDULER_INSTANCES = "http://localhost:8080"
npm run dev
```

实例列表默认从 `web/public/runtime-config.js` 读取，部署后可直接修改；`VITE_SCHEDULER_INSTANCES` 作为回退配置。

## 可观测性

- `GET /metrics`：Prometheus/OpenMetrics 指标，包括提交、派发、成功、失败、重入队、队列深度、在线 Worker、主节点状态、调度延迟和数据库延迟。
- `GET /api/v1/observability/overview`：管理台使用的当前节点概览。
- `GET /api/v1/observability/dashboard`：管理台直接使用的监控快照与滚动历史，不依赖 Prometheus。
- `GET /api/v1/events`：最近 200 条结构化调度事件，便于快速定位任务提交、派发、失败和租约过期。
- 所有 HTTP 响应带 `X-Request-ID`，前端或网关可透传自定义 request id。

生产环境建议将 `/metrics` 接入 Prometheus，将任务失败率、队列深度、Worker 在线数和 `scheduler_leader_state` 配置告警，并把结构化事件转发到日志平台长期留存。

管理台的“监控面板”直接读取 `/api/v1/observability/dashboard`，展示队列深度、运行中任务、任务状态分布、累计派发/成功/失败、平均调度延迟、Worker 利用率和实时事件；Prometheus 只是可选的外部监控出口。

## 安全配置

```text
SCHEDULER_API_TOKENS=admin-token:tenant-a:admin,operator-token:tenant-a:operator
SCHEDULER_WORKER_TOKENS=worker-token:tenant-a:worker
GRPC_CERT_FILE=/etc/scheduler/tls/server.crt
GRPC_KEY_FILE=/etc/scheduler/tls/server.key
```

HTTP API 支持 viewer/operator/admin，Worker gRPC 注册支持 token；配置证书后启用 TLS。生产环境应将 token 与证书放入密钥管理系统。

## 性能基准

```powershell
go test -bench=. ./internal/timewheel ./internal/scheduler
./scripts/load_test.ps1 -Count 10000
```
