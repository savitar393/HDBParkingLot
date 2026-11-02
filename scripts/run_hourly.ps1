# Optional Windows Task Scheduler entrypoint; runs a single collector cycle.
# Does not start a daemon or any retraining job.
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $repo
$logDir = Join-Path $repo 'data\quality\logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$log = Join-Path $logDir ('hourly-' + (Get-Date -Format 'yyyyMMddTHHmmss') + '.log')
& docker compose run --rm -T dev python scripts/collect_hourly.py --lookback-hours 4 2>&1 |
    Tee-Object -FilePath $log
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
