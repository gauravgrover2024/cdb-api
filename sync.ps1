# sync.ps1 - Run this daily to get main's latest changes into vipin
# Usage: .\sync.ps1

$branch = git rev-parse --abbrev-ref HEAD
if ($branch -ne "vipin") {
    Write-Host "WARNING: You are on '$branch', not 'vipin'. Switch first." -ForegroundColor Yellow
    exit 1
}

Write-Host "Fetching latest from origin..." -ForegroundColor Cyan
git fetch origin

$behind = git rev-list --count HEAD..origin/main
if ($behind -eq "0") {
    Write-Host "Already up to date with main. Nothing to do." -ForegroundColor Green
    exit 0
}

Write-Host "$behind new commit(s) on main. Rebasing vipin on top of main..." -ForegroundColor Cyan
git rebase origin/main

if ($LASTEXITCODE -ne 0) {
    Write-Host "Rebase conflict! Resolve conflicts, then run: git rebase --continue" -ForegroundColor Red
    exit 1
}

Write-Host "Pushing updated vipin to remote..." -ForegroundColor Cyan
git push origin vipin --force-with-lease

Write-Host "Done! vipin is now up to date with main." -ForegroundColor Green
