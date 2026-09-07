Write-Host "Groupe 1: 2009-2016" -ForegroundColor Cyan
$seasons1 = @(
    # "2009-2010", "2010-2011",
     "2011-2012", "2012-2013", "2013-2014", "2014-2015", "2015-2016",
"2016-2017", "2017-2018", "2018-2019", "2019-2020", "2020-2021", "2021-2022", "2022-2023", "2023-2024",
"2024-2025", "2025-2026"
)
foreach ($season in $seasons1) {
    Write-Host "  qualifiers + events_qual pour $season..." -ForegroundColor Yellow
    $vars = '{"target_season": "' + $season + '"}'
    dbt run -s player_match_stats --vars $vars
    # dbt run -s events_qual --vars $vars
    if ($LASTEXITCODE -ne 0) { exit 1 }
}


Write-Host "Done!" -ForegroundColor Green