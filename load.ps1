param(
    [string]$Project = "dag-failure-agent-v2",
    [string]$DashboardUrl = "https://dag-failure-agent-692925158254.us-central1.run.app"
)

Write-Host "Active gcloud account:"
gcloud auth list --filter=status:ACTIVE --format="value(account)"

Write-Host "Looking for Composer environments in project '$Project'..."

$envs = gcloud composer environments list --project=$Project --locations=- --format="csv[no-heading](name,location)" 2>$null

if (-not $envs) {
    Write-Host "No Composer environments found in project '$Project' visible to the active account." -ForegroundColor Red
    Write-Host "Check: gcloud auth list  (is this the right account?)" -ForegroundColor Yellow
    Write-Host "Check: gcloud config get-value project  (is this the right project?)" -ForegroundColor Yellow
    Write-Host "Check: gcloud projects list  (does this account even have access to '$Project'?)" -ForegroundColor Yellow
    exit 1
}

$envList = $envs -split "`n" | Where-Object { $_.Trim() -ne "" } | ForEach-Object {
    $parts = $_ -split ","
    [PSCustomObject]@{ Name = $parts[0]; Location = $parts[1] }
}

$chosen = $null
if ($envList.Count -eq 1) {
    $chosen = $envList[0]
    Write-Host "Found one environment: $($chosen.Name) ($($chosen.Location))"
} else {
    Write-Host "Multiple Composer environments found:"
    for ($i = 0; $i -lt $envList.Count; $i++) {
        Write-Host "  [$i] $($envList[$i].Name)  ($($envList[$i].Location))"
    }
    $selection = Read-Host "Enter the number of the environment to open"
    $chosen = $envList[[int]$selection]
}

Write-Host "Fetching Airflow UI URL for '$($chosen.Name)' in '$($chosen.Location)'..."

$AIRFLOW_URL = gcloud composer environments describe $chosen.Name `
    --location=$chosen.Location --project=$Project `
    --format="value(config.airflowUri)"

if (-not $AIRFLOW_URL) {
    Write-Host "Environment was found in the list but describe still failed. Try running the describe command manually to see the full error." -ForegroundColor Red
} else {
    Write-Host "Opening Airflow UI: $AIRFLOW_URL"
    Start-Process $AIRFLOW_URL
}

Write-Host "Opening dashboard: $DashboardUrl"
Start-Process $DashboardUrl