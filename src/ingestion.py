"""
ingestion.py
Extrae datos de CoinGecko API, los almacena en SQLite y genera evidencias.
"""

import requests
import sqlite3
import pandas as pd
from datetime import datetime
import os

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH       = os.path.join(BASE_DIR, "db", "ingestion.db")
XLSX_PATH     = os.path.join(BASE_DIR, "xlsx", "muestra.csv")
AUDIT_PATH    = os.path.join(BASE_DIR, "static", "auditoria", "ingestion.txt")

# ── 1. Extracción desde la API ─────────────────────────────────────────────────
def fetch_coins() -> list[dict]:
    """Consulta CoinGecko y retorna lista de monedas con sus métricas."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"[API] Registros extraídos: {len(data)}")
    return data

# ── 2. Almacenamiento en SQLite ────────────────────────────────────────────────
def store_in_db(coins: list[dict]) -> int:
    """Crea la tabla (si no existe) e inserta / actualiza registros. Retorna filas insertadas."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS coins (
            id                  TEXT PRIMARY KEY,
            symbol              TEXT,
            name                TEXT,
            current_price       REAL,
            market_cap          REAL,
            market_cap_rank     INTEGER,
            total_volume        REAL,
            high_24h            REAL,
            low_24h             REAL,
            price_change_24h    REAL,
            price_change_pct_24h REAL,
            circulating_supply  REAL,
            total_supply        REAL,
            ath                 REAL,
            last_updated        TEXT,
            ingested_at         TEXT
        )
    """)

    inserted = 0
    now = datetime.utcnow().isoformat()
    for c in coins:
        cur.execute("""
            INSERT INTO coins VALUES (
                :id, :symbol, :name, :current_price, :market_cap,
                :market_cap_rank, :total_volume, :high_24h, :low_24h,
                :price_change_24h, :price_change_percentage_24h,
                :circulating_supply, :total_supply, :ath,
                :last_updated, :ingested_at
            )
            ON CONFLICT(id) DO UPDATE SET
                current_price        = excluded.current_price,
                market_cap           = excluded.market_cap,
                market_cap_rank      = excluded.market_cap_rank,
                total_volume         = excluded.total_volume,
                high_24h             = excluded.high_24h,
                low_24h              = excluded.low_24h,
                price_change_24h     = excluded.price_change_24h,
                price_change_pct_24h = excluded.price_change_pct_24h,
                circulating_supply   = excluded.circulating_supply,
                total_supply         = excluded.total_supply,
                ath                  = excluded.ath,
                last_updated         = excluded.last_updated,
                ingested_at          = excluded.ingested_at
        """, {**c, "ingested_at": now})
        inserted += 1

    conn.commit()
    total_db = cur.execute("SELECT COUNT(*) FROM coins").fetchone()[0]
    conn.close()
    print(f"[DB]  Registros procesados: {inserted} | Total en BD: {total_db}")
    return total_db

# ── 3. Archivo de muestra CSV ──────────────────────────────────────────────────
def generate_sample():
    """Lee la BD y exporta las primeras 20 filas a CSV."""
    os.makedirs(os.path.dirname(XLSX_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM coins ORDER BY market_cap_rank LIMIT 20", conn
    )
    conn.close()
    df.to_csv(XLSX_PATH, index=False, encoding="utf-8")
    print(f"[CSV] Muestra guardada en: {XLSX_PATH}")
    return df

# ── 4. Archivo de auditoría ────────────────────────────────────────────────────
def generate_audit(api_coins: list[dict], db_total: int):
    """Compara los datos del API con los almacenados en la BD y genera reporte."""
    os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    df_db = pd.read_sql_query("SELECT id, name, current_price, market_cap_rank FROM coins", conn)
    conn.close()

    api_ids = {c["id"] for c in api_coins}
    db_ids  = set(df_db["id"].tolist())

    only_in_api = api_ids - db_ids
    only_in_db  = db_ids  - api_ids
    in_both     = api_ids & db_ids

    lines = [
        "=" * 60,
        "       REPORTE DE AUDITORÍA — INGESTA COINGECKO",
        "=" * 60,
        f"Fecha/Hora UTC  : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Fuente          : CoinGecko /coins/markets (top 100 por market cap)",
        "",
        "── CONTEO DE REGISTROS ──────────────────────────────────",
        f"  Extraídos del API   : {len(api_coins)}",
        f"  Almacenados en BD   : {db_total}",
        f"  En común (API ∩ BD) : {len(in_both)}",
        f"  Solo en API         : {len(only_in_api)}",
        f"  Solo en BD          : {len(only_in_db)}",
        "",
        "── INTEGRIDAD ───────────────────────────────────────────",
    ]

    if len(only_in_api) == 0 and len(only_in_db) == 0:
        lines.append("  ✅ Todos los registros del API están en la BD. Integridad OK.")
    else:
        if only_in_api:
            lines.append(f"  ⚠️  IDs solo en API : {', '.join(list(only_in_api)[:10])}")
        if only_in_db:
            lines.append(f"  ⚠️  IDs solo en BD  : {', '.join(list(only_in_db)[:10])}")

    lines += [
        "",
        "── TOP 10 POR MARKET CAP (BD) ───────────────────────────",
    ]
    top10 = df_db.sort_values("market_cap_rank").head(10)
    for _, row in top10.iterrows():
        lines.append(f"  #{int(row['market_cap_rank']):>3}  {row['name']:<20} id={row['id']}")

    lines += ["", "=" * 60, "Fin del reporte.", "=" * 60]

    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"[AUD] Auditoría guardada en: {AUDIT_PATH}")

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🚀 Iniciando ingesta de datos...\n")
    coins    = fetch_coins()
    db_total = store_in_db(coins)
    generate_sample()
    generate_audit(coins, db_total)
    print("\n✅ Ingesta completada.\n")
