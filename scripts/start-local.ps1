param(
  [int]$HttpPort = 18080,
  [int]$GrpcPort = 19090,
  [int]$FrontendPort = 8780,
  [switch]$WithPostgres
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$runtime = Join-Path $repo '.runtime'
New-Item -ItemType Directory -Force -Path $runtime | Out-Null

function Stop-Tree([int]$Pid) {
  if (Get-Process -Id $Pid -ErrorAction SilentlyContinue) {
    & taskkill.exe /PID $Pid /T /F | Out-Null
  }
}

& (Join-Path $PSScriptRoot 'stop-local.ps1') -Quiet

if ($WithPostgres) { docker compose up -d postgres | Out-Host }

Push-Location $repo
try {
  go build -o (Join-Path $runtime 'scheduler.exe') ./cmd/scheduler
  go build -o (Join-Path $runtime 'demo-worker.exe') ./cmd/demo-worker
} finally { Pop-Location }

$scheduler = Start-Process -FilePath (Join-Path $runtime 'scheduler.exe') -WorkingDirectory $repo -Environment @{ HTTP_ADDR = ":$HttpPort"; GRPC_ADDR = ":$GrpcPort"; SCHEDULER_TRACE_STDOUT = '0' } -RedirectStandardOutput (Join-Path $runtime 'scheduler.log') -RedirectStandardError (Join-Path $runtime 'scheduler.err.log') -PassThru
Set-Content -LiteralPath (Join-Path $runtime 'scheduler.pid') -Value $scheduler.Id

$worker = Start-Process -FilePath (Join-Path $runtime 'demo-worker.exe') -WorkingDirectory $repo -Environment @{ DEMO_GRPC_ENDPOINT = "localhost:$GrpcPort"; DEMO_HEALTH_PORT = '18081' } -RedirectStandardOutput (Join-Path $runtime 'worker.log') -RedirectStandardError (Join-Path $runtime 'worker.err.log') -PassThru
Set-Content -LiteralPath (Join-Path $runtime 'worker.pid') -Value $worker.Id

$frontend = Start-Process -FilePath 'npm.cmd' -ArgumentList 'run','dev','--','--host','0.0.0.0','--port',"$FrontendPort" -WorkingDirectory (Join-Path $repo 'web') -Environment @{ VITE_SCHEDULER_INSTANCES = "http://localhost:$HttpPort" } -RedirectStandardOutput (Join-Path $runtime 'frontend.log') -RedirectStandardError (Join-Path $runtime 'frontend.err.log') -PassThru
Set-Content -LiteralPath (Join-Path $runtime 'frontend.pid') -Value $frontend.Id

& (Join-Path $PSScriptRoot 'health-local.ps1') -HttpPort $HttpPort -FrontendPort $FrontendPort -WaitSeconds 60
