param(
  [int]$HttpPort = 18080,
  [int]$FrontendPort = 8780,
  [int]$WaitSeconds = 60
)

$ErrorActionPreference = 'Stop'
$checks = @(
  @{ Name = 'scheduler'; Url = "http://localhost:$HttpPort/healthz" },
  @{ Name = 'ready'; Url = "http://localhost:$HttpPort/readyz" },
  @{ Name = 'frontend'; Url = "http://localhost:$FrontendPort/" }
)
for ($i = 0; $i -lt $WaitSeconds * 2; $i++) {
  $results = @{}
  $ok = $true
  foreach ($check in $checks) {
    try { $response = Invoke-WebRequest -Uri $check.Url -TimeoutSec 10; $results[$check.Name] = $response.StatusCode; if ($response.StatusCode -lt 200 -or $response.StatusCode -ge 400) { $ok = $false } }
    catch { $results[$check.Name] = 'down'; $ok = $false }
  }
  if ($ok) { $results | ConvertTo-Json -Compress; exit 0 }
  Start-Sleep -Milliseconds 500
}
$results | ConvertTo-Json -Compress
throw 'local system health check failed'
