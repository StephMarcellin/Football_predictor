# wait_for_prefect.ps1
# Attend que le serveur Prefect soit disponible avant de continuer.
# Appelé par le Makefile après "start /B prefect server start".

$url     = "http://127.0.0.1:4200/api/health"
$max     = 30   # secondes max d'attente
$elapsed = 0

Write-Host "⏳ Attente du serveur Prefect..." -ForegroundColor Cyan

while ($elapsed -lt $max) {
    try {
        $response = Invoke-WebRequest -Uri $url -TimeoutSec 1 -ErrorAction Stop
        Write-Host "✅ Prefect disponible après ${elapsed}s" -ForegroundColor Green
        exit 0   # succès — Make continue
    }
    catch {
        Start-Sleep -Seconds 1
        $elapsed++
    }
}

# Si on arrive ici, le serveur n'a pas répondu dans les temps
Write-Host "⚠️  Prefect non disponible après ${max}s — le pipeline continue sans garantie de persistance des Artifacts." -ForegroundColor Yellow
exit 0   # on ne bloque pas le pipeline pour autant