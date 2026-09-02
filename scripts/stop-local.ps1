param([switch]$Quiet)
$ErrorActionPreference = 'SilentlyContinue'
$repo = Split-Path -Parent $PSScriptRoot
$runtime = Join-Path $repo '.runtime'
foreach ($name in @('frontend','worker','scheduler')) {
  $pidFile = Join-Path $runtime "$name.pid"
  if (Test-Path -LiteralPath $pidFile) {
    $processId = [int](Get-Content -LiteralPath $pidFile)
    & taskkill.exe /PID $processId /T /F | Out-Null
    Remove-Item -LiteralPath $pidFile -Force
    if (-not $Quiet) { "stopped $name ($processId)" }
  }
}
