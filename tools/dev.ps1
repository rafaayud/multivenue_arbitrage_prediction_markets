param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "stop", "check")]
    [string]$Command = "start",
    [switch]$Check
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$frontendRoot = Join-Path $projectRoot "frontend"
$vite = Join-Path $frontendRoot "node_modules\vite\bin\vite.js"
$environmentFile = Join-Path $projectRoot ".env"
$prometheusConfig = Join-Path $projectRoot "prometheus.local.yml"

function Stop-PortProcess {
    param(
        [int]$Port
    )

    $portPattern = "^\s*TCP\s+\S+:$Port\s+\S+\s+LISTENING\s+(\d+)\s*$"
    $connection = netstat.exe -ano -p tcp |
        Select-String -Pattern $portPattern |
        Select-Object -First 1
    if (-not $connection) {
        return
    }

    $ownerId = [int]$connection.Matches[0].Groups[1].Value
    taskkill.exe /PID $ownerId /T /F 2>$null | Out-Null
}

function Stop-DockerServices {
    Push-Location $projectRoot
    try {
        docker compose down
        if ($LASTEXITCODE -ne 0) {
            throw "Docker services failed to stop."
        }
    }
    finally {
        Pop-Location
    }
}

function Assert-DevelopmentEnvironment {
    if (-not (Get-Command docker.exe -ErrorAction SilentlyContinue)) {
        throw "Docker is not installed or is not available in PATH."
    }
    if (-not (Get-Command node.exe -ErrorAction SilentlyContinue)) {
        throw "Node.js is not installed or is not available in PATH."
    }
    if (-not (Test-Path -LiteralPath $python)) {
        throw "Python environment is missing. Run: uv sync"
    }
    if (-not (Test-Path -LiteralPath $vite)) {
        throw "Frontend dependencies are missing. Run: cd frontend; pnpm.cmd install"
    }
    if (-not (Test-Path -LiteralPath $environmentFile)) {
        throw ".env is missing. Copy .env.example and configure it first."
    }
    if (-not (Test-Path -LiteralPath $prometheusConfig)) {
        throw "prometheus.local.yml is missing."
    }
}

function Assert-PortAvailable {
    param(
        [int]$Port
    )

    $portPattern = "^\s*TCP\s+\S+:$Port\s+\S+\s+LISTENING\s+(\d+)\s*$"
    $connection = netstat.exe -ano -p tcp |
        Select-String -Pattern $portPattern |
        Select-Object -First 1
    if ($connection) {
        $ownerId = [int]$connection.Matches[0].Groups[1].Value
        $owner = Get-Process -Id $ownerId -ErrorAction SilentlyContinue
        $description = if ($owner) {
            "$($owner.ProcessName) (PID $($owner.Id))"
        }
        else {
            "PID $ownerId"
        }
        throw "Port $Port is already in use by $description."
    }

    $probe = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Loopback,
        $Port
    )
    try {
        $probe.Start()
    }
    catch {
        throw "Port $Port is not available."
    }
    finally {
        $probe.Stop()
    }
}

function Start-ManagedProcess {
    param(
        [string]$Name,
        [string]$FilePath,
        [string]$Arguments,
        [string]$WorkingDirectory
    )

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.Arguments = $Arguments
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw "Failed to start $Name."
    }

    return [pscustomobject]@{
        Name = $Name
        Process = $process
    }
}

function Stop-ManagedProcess {
    param(
        [object]$ManagedProcess
    )

    $process = $ManagedProcess.Process
    try {
        if (-not $process.HasExited) {
            taskkill.exe /PID $process.Id /T /F 2>$null | Out-Null
            $process.WaitForExit(5000) | Out-Null
        }
    }
    finally {
        $process.Dispose()
    }
}

if ($Command -eq "stop") {
    if (-not (Get-Command docker.exe -ErrorAction SilentlyContinue)) {
        throw "Docker is not installed or is not available in PATH."
    }

    Write-Host "Stopping FastAPI and Vite..." -ForegroundColor Yellow
    Stop-PortProcess 8000
    Stop-PortProcess 8001
    Stop-PortProcess 5173
    Write-Host "Stopping PostgreSQL and Prometheus..." -ForegroundColor Yellow
    Stop-DockerServices
    Write-Host "Development environment stopped." -ForegroundColor Green
    exit 0
}

Assert-DevelopmentEnvironment
Assert-PortAvailable 8000
Assert-PortAvailable 8001
Assert-PortAvailable 5173
$env:PROMETHEUS_CONFIG_PATH = "./prometheus.local.yml"

Push-Location $projectRoot
try {
    docker compose config --quiet
    if ($LASTEXITCODE -ne 0) {
        throw "docker-compose.yml is invalid."
    }

    if ($Check -or $Command -eq "check") {
        Write-Host "Development environment is ready." -ForegroundColor Green
        exit 0
    }

    Write-Host "Starting PostgreSQL and Prometheus..." -ForegroundColor Cyan
    docker compose up -d postgres prometheus
    if ($LASTEXITCODE -ne 0) {
        throw "Docker services failed to start."
    }
}
finally {
    Pop-Location
}

$node = (Get-Command node.exe).Source
$processes = @()

try {
    $processes += Start-ManagedProcess `
        -Name "Control API" `
        -FilePath $python `
        -Arguments "-m uvicorn prediction_markets.api.control_main:app --host 127.0.0.1 --port 8000" `
        -WorkingDirectory $projectRoot
    $processes += Start-ManagedProcess `
        -Name "Trading API" `
        -FilePath $python `
        -Arguments "-m uvicorn prediction_markets.api.trading_main:app --host 127.0.0.1 --port 8001" `
        -WorkingDirectory $projectRoot
    $processes += Start-ManagedProcess `
        -Name "Vite" `
        -FilePath $node `
        -Arguments "node_modules\vite\bin\vite.js --host 127.0.0.1 --strictPort" `
        -WorkingDirectory $frontendRoot

    Write-Host ""
    Write-Host "Prediction Markets is starting" -ForegroundColor Green
    Write-Host "  Dashboard:  http://localhost:5173"
    Write-Host "  Control API: http://localhost:8000"
    Write-Host "  Trading API: http://localhost:8001"
    Write-Host "  API docs:    http://localhost:8000/docs"
    Write-Host "  Prometheus:  http://localhost:9090"
    Write-Host ""
    Write-Host "Press Ctrl+C to stop both APIs and Vite." -ForegroundColor Yellow
    Write-Host ""

    while ($true) {
        $stoppedProcess = $processes |
            Where-Object { $_.Process.HasExited } |
            Select-Object -First 1
        if ($stoppedProcess) {
            throw "$($stoppedProcess.Name) stopped with exit code $($stoppedProcess.Process.ExitCode)."
        }

        Start-Sleep -Milliseconds 200
    }
}
finally {
    Write-Host ""
    Write-Host "Stopping FastAPI and Vite..." -ForegroundColor Yellow
    foreach ($process in $processes) {
        Stop-ManagedProcess $process
    }
    Write-Host "Stopping PostgreSQL and Prometheus..." -ForegroundColor Yellow
    Stop-DockerServices
}
