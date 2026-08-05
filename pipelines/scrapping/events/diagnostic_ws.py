# diagnostic_ws.py — à placer à la racine du projet
from seleniumbase import Driver
import json, re
from pathlib import Path

driver = Driver(uc=True, headless=False)

# Injecter les cookies
driver.get("https://www.whoscored.com")
import time; time.sleep(4)

with open("config/whoscored_cookies.json", encoding="utf-8") as f:
    cookies = json.load(f)
for cookie in cookies:
    cookie.pop("sameSite", None)
    cookie.pop("storeId", None)
    cookie.pop("hostOnly", None)
    try:
        driver.add_cookie(cookie)
    except Exception:
        pass

driver.get("https://www.whoscored.com/matches/1190175/live/england-premier-league-2017-2018-brighton-manchester-city")
time.sleep(8)

# Sauvegarder le source complet
source = driver.page_source
Path("diagnostic_source.html").write_text(source, encoding="utf-8")
print("Source sauvegardé dans diagnostic_source.html")

# Chercher toutes les variables JS qui contiennent des données
patterns = [
    r"var\s+(\w+)\s*=\s*\{",          # var quelqueChose = {
    r"window\.(\w+)\s*=\s*\{",        # window.quelqueChose = {
    r"(\w+)\s*=\s*JSON\.parse\(",      # quelqueChose = JSON.parse(
]
for pat in patterns:
    matches = re.findall(pat, source)
    if matches:
        print(f"\nPattern `{pat}` :")
        for m in matches:
            print(f"  → {m}")
# Ajoute ces lignes à la fin de diagnostic_ws.py avant driver.quit()

# Chercher des mots-clés liés aux stats de match
keywords = [
    "attemptTypes", "openPlay", "setpiece", "keyPasses", 
    "throughBalls", "yellowCard", "attackZones", "fieldZones",
    "matchCentreData", "matchCentreEventTypeJson",
    "initialMatchDataForConsumption", "matchHeaderData",
    "requirejs", "wsData", "matchData"
]

print("\n=== Recherche de mots-clés dans le source ===")
for kw in keywords:
    if kw in source:
        # Montrer le contexte autour du mot-clé
        idx = source.index(kw)
        print(f"\n✅ '{kw}' trouvé à index {idx} :")
        print(f"   ...{source[max(0,idx-80):idx+80]}...")
    else:
        print(f"❌ '{kw}' absent")

# Ajoute ces lignes à diagnostic_ws.py

import json, re

# Extraire matchCentreData
pattern = re.compile(
    r'matchCentreData\s*:\s*(\{.*?\})\s*,\s*\n\s*matchCentreEventTypeJson',
    re.DOTALL
)
m = pattern.search(source)
if m:
    try:
        data = json.loads(m.group(1))
        # Sauvegarder pour inspection
        Path("diagnostic_matchcentre.json").write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        print("\n✅ matchCentreData extrait et sauvegardé dans diagnostic_matchcentre.json")
        print(f"   Clés racine : {list(data.keys())}")
    except json.JSONDecodeError as e:
        print(f"❌ JSON invalide : {e}")
        print(f"   Début : {m.group(1)[:200]}")
else:
    print("❌ Pattern matchCentreData non trouvé")

driver.quit()