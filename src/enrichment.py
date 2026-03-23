"""
enrichment.py
EA3 - Enriquecimiento de Datos
Lee el dataset limpio de EA2 y lo enriquece con 6 fuentes adicionales:
  - JSON  : categorías y sector
  - CSV   : métricas de riesgo
  - XML   : información regulatoria
  - TXT   : sentimiento de mercado
  - XLSX  : información del proyecto
  - HTML  : listados en exchanges
Genera: enriched_data.csv y enrichment_report.txt
"""

import os
import sqlite3
import pandas as pd
import json
import xml.etree.ElementTree as ET
from datetime import datetime

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DB_PATH      = os.path.join(BASE_DIR, "db",     "ingestion.db")
SOURCES_DIR  = os.path.join(BASE_DIR, "data_sources")
OUTPUT_CSV   = os.path.join(BASE_DIR, "xlsx",   "enriched_data.csv")
REPORT_PATH  = os.path.join(BASE_DIR, "static", "auditoria", "enrichment_report.txt")

os.makedirs(os.path.dirname(OUTPUT_CSV),   exist_ok=True)
os.makedirs(os.path.dirname(REPORT_PATH),  exist_ok=True)

# ── 1. Cargar dataset base (EA2 cleaned_data) ─────────────────────────────────
def load_base() -> pd.DataFrame:
    print("[BASE] Cargando dataset limpio desde SQLite (EA2)...")
    # Intentar desde CSV limpio primero, luego desde BD
    cleaned_csv = os.path.join(BASE_DIR, "xlsx", "cleaned_data.csv")
    if os.path.exists(cleaned_csv):
        df = pd.read_csv(cleaned_csv)
        print(f"[BASE] Cargado desde cleaned_data.csv: {len(df)} registros")
    else:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM coins", conn)
        conn.close()
        print(f"[BASE] Cargado desde ingestion.db: {len(df)} registros")
    return df

# ── 2. Leer fuentes adicionales ───────────────────────────────────────────────

def read_json() -> pd.DataFrame:
    path = os.path.join(SOURCES_DIR, "crypto_categories.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    print(f"[JSON] Leídos {len(df)} registros desde crypto_categories.json")
    return df

def read_csv() -> pd.DataFrame:
    path = os.path.join(SOURCES_DIR, "risk_metrics.csv")
    df = pd.read_csv(path)
    print(f"[CSV]  Leídos {len(df)} registros desde risk_metrics.csv")
    return df

def read_xml() -> pd.DataFrame:
    path = os.path.join(SOURCES_DIR, "regulatory_info.xml")
    tree = ET.parse(path)
    root = tree.getroot()
    rows = []
    for coin in root.findall("coin"):
        rows.append({
            "id":           coin.get("id"),
            "legal_status": coin.findtext("legal_status"),
            "regulated":    coin.findtext("regulated"),
            "main_exchange":coin.findtext("main_exchange"),
        })
    df = pd.DataFrame(rows)
    print(f"[XML]  Leídos {len(df)} registros desde regulatory_info.xml")
    return df

def read_txt() -> pd.DataFrame:
    path = os.path.join(SOURCES_DIR, "sentiment_data.txt")
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            if len(parts) == 4:
                rows.append({
                    "id":                   parts[0],
                    "sentiment_score":      float(parts[1]),
                    "sentiment_label":      parts[2],
                    "social_mentions_24h":  int(parts[3]),
                })
    df = pd.DataFrame(rows)
    print(f"[TXT]  Leídos {len(df)} registros desde sentiment_data.txt")
    return df

def read_xlsx() -> pd.DataFrame:
    path = os.path.join(SOURCES_DIR, "project_info.xlsx")
    df = pd.read_excel(path, engine="openpyxl")
    print(f"[XLSX] Leídos {len(df)} registros desde project_info.xlsx")
    return df

def read_html() -> pd.DataFrame:
    path = os.path.join(SOURCES_DIR, "exchange_listings.html")
    tables = pd.read_html(path)
    df = tables[0]
    # Asegurar tipos correctos
    for col in ["listed_exchanges", "trading_pairs", "listing_year", "cmc_rank_2023"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    print(f"[HTML] Leídos {len(df)} registros desde exchange_listings.html")
    return df

# ── 3. Enriquecimiento (merges) ───────────────────────────────────────────────
def enrich(base: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {}
    stats["base_records"] = len(base)
    df = base.copy()

    sources = [
        ("JSON - Categorías",      read_json(),  ["category","sector","consensus","founded_year"]),
        ("CSV  - Riesgo",          read_csv(),   ["risk_score","volatility_30d","sharpe_ratio","beta"]),
        ("XML  - Regulación",      read_xml(),   ["legal_status","regulated","main_exchange"]),
        ("TXT  - Sentimiento",     read_txt(),   ["sentiment_score","sentiment_label","social_mentions_24h"]),
        ("XLSX - Proyecto",        read_xlsx(),  ["whitepaper_year","team_size","github_commits_30d","partnerships_count"]),
        ("HTML - Exchanges",       read_html(),  ["listed_exchanges","trading_pairs","listing_year","cmc_rank_2023"]),
    ]

    source_stats = []
    for name, src_df, cols in sources:
        before = len(df)
        merge_cols = ["id"] + [c for c in cols if c in src_df.columns]
        src_sub = src_df[merge_cols].copy()
        df = df.merge(src_sub, on="id", how="left")
        matched = df[cols[0]].notna().sum()
        source_stats.append({
            "source":   name,
            "records":  len(src_df),
            "matched":  int(matched),
            "cols_added": len(cols),
        })
        print(f"[MERGE] {name}: {matched}/{before} registros coincidentes")

    stats["enriched_records"] = len(df)
    stats["total_columns"]    = len(df.columns)
    stats["source_stats"]     = source_stats

    # Transformaciones adicionales
    df["investment_score"] = (
        (1 - df["risk_score"].fillna(5) / 10) * 0.3 +
        df["sentiment_score"].fillna(0) * 0.3 +
        (df["github_commits_30d"].fillna(0) / df["github_commits_30d"].max()) * 0.2 +
        (df["partnerships_count"].fillna(0) / df["partnerships_count"].max()) * 0.2
    ).round(4)

    stats["investment_score_added"] = True
    return df, stats

# ── 4. Exportar CSV ───────────────────────────────────────────────────────────
def export_csv(df: pd.DataFrame):
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"[EXPORT] enriched_data.csv guardado: {len(df)} registros, {len(df.columns)} columnas")

# ── 5. Reporte de auditoría ───────────────────────────────────────────────────
def generate_report(stats: dict, df: pd.DataFrame):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "=" * 65,
        "     REPORTE DE AUDITORÍA — ENRIQUECIMIENTO EA3",
        "=" * 65,
        f"Fecha/Hora UTC    : {now}",
        f"Dataset base      : cleaned_data.csv / ingestion.db (EA2)",
        "",
        "── CONTEO DE REGISTROS ──────────────────────────────────",
        f"  Registros base (EA2)     : {stats['base_records']}",
        f"  Registros enriquecidos   : {stats['enriched_records']}",
        f"  Columnas antes           : 17",
        f"  Columnas después         : {stats['total_columns']}",
        f"  Columnas nuevas añadidas : {stats['total_columns'] - 17}",
        "",
        "── FUENTES INTEGRADAS ───────────────────────────────────",
    ]

    for s in stats["source_stats"]:
        lines += [
            f"  {s['source']}",
            f"    Registros en fuente  : {s['records']}",
            f"    Coincidentes (match) : {s['matched']}",
            f"    Columnas añadidas    : {s['cols_added']}",
            "",
        ]

    lines += [
        "── TRANSFORMACIONES ADICIONALES ─────────────────────────",
        "  ✔ Columna 'investment_score' calculada:",
        "      = (1 - risk_score/10)*0.3 + sentiment_score*0.3",
        "        + github_commits_norm*0.2 + partnerships_norm*0.2",
        "",
        "── TOP 10 POR INVESTMENT SCORE ──────────────────────────",
    ]

    top10 = df.sort_values("investment_score", ascending=False).head(10)
    for _, row in top10.iterrows():
        lines.append(
            f"  {row['name']:<20} score={row['investment_score']:.4f}"
            f"  sentiment={row.get('sentiment_label','N/A')}"
            f"  risk={row.get('risk_score','N/A')}"
        )

    lines += [
        "",
        "── DISTRIBUCIÓN POR SECTOR ──────────────────────────────",
    ]
    if "sector" in df.columns:
        for sector, count in df["sector"].value_counts().items():
            lines.append(f"  {sector:<20}: {count} monedas")

    lines += [
        "",
        "── DISTRIBUCIÓN POR SENTIMIENTO ─────────────────────────",
    ]
    if "sentiment_label" in df.columns:
        for label, count in df["sentiment_label"].value_counts().items():
            lines.append(f"  {label:<15}: {count} monedas")

    lines += ["", "=" * 65, "Fin del reporte.", "=" * 65]

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[REPORT] enrichment_report.txt guardado.")

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🚀 EA3 - Enriquecimiento de Datos\n")
    base_df          = load_base()
    enriched_df, stats = enrich(base_df)
    export_csv(enriched_df)
    generate_report(stats, enriched_df)
    print("\n✅ Enriquecimiento completado.\n")
