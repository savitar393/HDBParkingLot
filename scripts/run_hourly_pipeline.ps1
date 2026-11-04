$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $repo
$logDir = Join-Path $repo 'data\quality\logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$log = Join-Path $logDir ('pipeline-' + (Get-Date -Format 'yyyyMMddTHHmmss') + '.log')
# Prevent overlap between invocations of this PowerShell wrapper on the same host.
$mutex = [System.Threading.Mutex]::new($false, 'Global\HDBParkingHourlyPipeline')
$acquired = $false
try {
    $acquired = $mutex.WaitOne(0)
    if (-not $acquired) {
        Write-Error 'Another HDB hourly pipeline invocation is still running.'
        exit 2
    }
    & docker compose run --rm -T dev python scripts/run_hourly_pipeline.py --lookback-hours 4 2>&1 |
        Tee-Object -FilePath $log
    $code = $LASTEXITCODE
    if ($code -ne 0) { exit $code }
}
finally {
    if ($acquired) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
