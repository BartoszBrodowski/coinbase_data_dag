$ErrorActionPreference = "Stop"

function Invoke-Compose {
    param([string[]]$ComposeArguments)

    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        & docker-compose @ComposeArguments
    }
    elseif (Get-Command docker -ErrorAction SilentlyContinue) {
        & docker compose @ComposeArguments
    }
    else {
        throw "Docker Compose is required. Install Docker Desktop and try again."
    }

    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose failed with exit code $LASTEXITCODE."
    }
}

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$environmentFile = Join-Path $repositoryRoot ".env"

if (-not (Test-Path -LiteralPath $environmentFile)) {
    throw "Create .env from .env.example and replace every change-me value first."
}

Push-Location $repositoryRoot
try {
    Invoke-Compose @("config", "--quiet")
    Invoke-Compose @(
        "--profile", "test", "run", "--rm", "unit-tests",
        "python", "-m", "compileall", "-q", "dags", "dashboard", "tests"
    )
    Invoke-Compose @("--profile", "test", "run", "--rm", "unit-tests")
    Invoke-Compose @("--profile", "test", "run", "--rm", "integration-tests")
}
finally {
    Pop-Location
}
