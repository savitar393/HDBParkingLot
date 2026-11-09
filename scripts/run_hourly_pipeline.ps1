$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$logDir = Join-Path $repo 'data\quality\logs'
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$logPath = Join-Path $logDir ('pipeline-' + (Get-Date -Format 'yyyyMMddTHHmmss') + '.log')

# A non-blocking, user-session mutex prevents overlapping scheduled/manual cycles.
$mutex = [System.Threading.Mutex]::new($false, 'Local\HDBParkingLotCompleteHourlyPipeline')
$hasLock = $false
$exitCode = 1
try {
    try {
        $hasLock = $mutex.WaitOne(0)
    } catch [System.Threading.AbandonedMutexException] {
        # Previous owner died: we now own the abandoned mutex.
        $hasLock = $true
    }
    if (-not $hasLock) {
        Write-Output 'SKIPPED: another hourly pipeline cycle is still running.'
        $exitCode = 0
    } else {
        Set-Location $repo
        $docker = (Get-Command docker -ErrorAction Stop).Source
        Write-Output ('START: ' + (Get-Date -Format o))
        Write-Output ('REPO: ' + $repo)
        Write-Output ('LOG: ' + $logPath)
        # PowerShell 5.1 treats some native stderr output as nonterminating errors;
        # do not turn Spark warnings into terminating PowerShell exceptions.
        $priorPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = 'Continue'
            & $docker compose run --rm -T dev python scripts/run_hourly_pipeline.py --lookback-hours 4 2>&1 |
                ForEach-Object { $_.ToString() } |
                Tee-Object -FilePath $logPath
            $exitCode = [int]$LASTEXITCODE
        } finally {
            $ErrorActionPreference = $priorPreference
        }
        Write-Output ('END: ' + (Get-Date -Format o) + ' exit=' + $exitCode)
        if ($exitCode -ne 0) {
            Write-Warning ('Hourly pipeline failed; inspect ' + $logPath)
        }
    }
} catch {
    Write-Error -ErrorAction Continue ('Hourly launcher error: ' + $_.Exception.Message)
    $exitCode = 1
} finally {
    if ($hasLock) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
exit $exitCode
