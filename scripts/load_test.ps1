param([int]$Count = 10000, [string]$BaseUrl = 'http://localhost:18080', [int]$Concurrency = 16, [string]$Token = '')
$headers = @{}
if ($Token) { $headers['Authorization'] = "Bearer $Token" }
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$jobs = 1..$Count | ForEach-Object {
  $body = @{ task_name = 'load.echo'; role = 'default'; params = @{ sequence = $_ }; priority = ($_ % 10) } | ConvertTo-Json -Depth 5
  Start-Job -ScriptBlock { param($url, $payload, $headers); Invoke-RestMethod -Method Post -Uri $url -ContentType 'application/json' -Headers $headers -Body $payload | Out-Null } -ArgumentList "$BaseUrl/api/v1/tasks", $body, $headers
  if ($_ % $Concurrency -eq 0) { Get-Job | Wait-Job | Receive-Job | Out-Null; Get-Job | Remove-Job }
}
Get-Job | Wait-Job | Receive-Job | Out-Null; Get-Job | Remove-Job
$sw.Stop(); "submitted=$Count elapsed_ms=$($sw.ElapsedMilliseconds) throughput_per_sec=$([math]::Round($Count / $sw.Elapsed.TotalSeconds, 2))"
