"""
cleaning.py
EA2 - Preprocesamiento y Limpieza de Datos
Lee desde ingestion.db (simulando entorno cloud), limpia con PySpark + Pandas
y genera evidencias: cleaned_data.xlsx y cleaning_report.txt
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime

# PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(BASE_DIR, "db", "ingestion.db")
XLSX_PATH   = os.path.join(BASE_DIR, "xlsx", "cleaned_data.xlsx")
REPORT_PATH = os.path.join(BASE_DIR, "static", "auditoria", "cleaning_report.txt")

os.makedirs(os.path.dirname(XLSX_PATH),   exist_ok=True)
os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

# ── 1. Cargar datos desde SQLite (simulando entorno cloud) ────────────────────
def load_from_db() -> pd.DataFrame:
    print("[LOAD] Conectando a la base de datos (entorno cloud simulado)...")
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM coins", conn)
    conn.close()
    print(f"[LOAD] Registros cargados: {len(df)}")
    return df

# ── 2. Iniciar SparkSession ───────────────────────────────────────────────────
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EA2-Limpieza-CoinGecko")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("[SPARK] SparkSession iniciada en modo local (simula cluster cloud)")
    return spark

# ── 3. Análisis exploratorio inicial ─────────────────────────────────────────
def exploratory_analysis(sdf) -> dict:
    print("\n[EDA] Análisis exploratorio inicial...")
    stats = {}
    stats["total_records"]     = sdf.count()
    stats["total_columns"]     = len(sdf.columns)
    stats["duplicate_records"] = sdf.count() - sdf.dropDuplicates(["id"]).count()

    # Nulos por columna
    null_counts = {}
    for col in sdf.columns:
        n = sdf.filter(F.col(col).isNull()).count()
        null_counts[col] = n
    stats["nulls_per_column"] = null_counts
    stats["total_nulls"]      = sum(null_counts.values())

    # Estadísticas numéricas básicas
    numeric_cols = ["current_price", "market_cap", "total_volume",
                    "high_24h", "low_24h", "price_change_pct_24h", "ath"]
    stats["numeric_summary"] = (
        sdf.select(numeric_cols)
           .describe()
           .toPandas()
    )
    print(f"[EDA] Total registros   : {stats['total_records']}")
    print(f"[EDA] Duplicados        : {stats['duplicate_records']}")
    print(f"[EDA] Total nulos       : {stats['total_nulls']}")
    return stats

# ── 4. Limpieza y transformación ──────────────────────────────────────────────
def clean_data(sdf):
    print("\n[CLEAN] Iniciando limpieza...")
    ops = []

    # 4.1 Eliminar duplicados por id
    before = sdf.count()
    sdf = sdf.dropDuplicates(["id"])
    removed_dups = before - sdf.count()
    ops.append(f"Duplicados eliminados          : {removed_dups}")
    print(f"[CLEAN] Duplicados eliminados: {removed_dups}")

    # 4.2 Corrección de tipos de datos
    sdf = (
        sdf
        .withColumn("current_price",        F.col("current_price").cast(DoubleType()))
        .withColumn("market_cap",            F.col("market_cap").cast(DoubleType()))
        .withColumn("market_cap_rank",       F.col("market_cap_rank").cast(IntegerType()))
        .withColumn("total_volume",          F.col("total_volume").cast(DoubleType()))
        .withColumn("high_24h",              F.col("high_24h").cast(DoubleType()))
        .withColumn("low_24h",               F.col("low_24h").cast(DoubleType()))
        .withColumn("price_change_24h",      F.col("price_change_24h").cast(DoubleType()))
        .withColumn("price_change_pct_24h",  F.col("price_change_pct_24h").cast(DoubleType()))
        .withColumn("circulating_supply",    F.col("circulating_supply").cast(DoubleType()))
        .withColumn("total_supply",          F.col("total_supply").cast(DoubleType()))
        .withColumn("ath",                   F.col("ath").cast(DoubleType()))
        .withColumn("id",                    F.col("id").cast(StringType()))
        .withColumn("symbol",                F.upper(F.col("symbol")))   # símbolo en mayúsculas
        .withColumn("last_updated",          F.to_timestamp("last_updated"))
        .withColumn("ingested_at",           F.to_timestamp("ingested_at"))
    )
    ops.append("Tipos de datos corregidos      : cast numéricos, symbol a mayúsculas, fechas a timestamp")
    print("[CLEAN] Tipos de datos corregidos")

    # 4.3 Manejo de valores nulos
    #   - Numéricos: imputar con mediana (calculada en Pandas, aplicada en Spark)
    numeric_cols = ["current_price", "market_cap", "total_volume",
                    "high_24h", "low_24h", "price_change_24h",
                    "price_change_pct_24h", "circulating_supply",
                    "total_supply", "ath"]

    pdf_temp = sdf.select(numeric_cols).toPandas()
    medians  = pdf_temp.median()

    fill_map = {c: float(medians[c]) for c in numeric_cols}
    sdf = sdf.fillna(fill_map)

    #   - Textos: reemplazar nulos con 'unknown'
    sdf = sdf.fillna({"symbol": "UNKNOWN", "name": "Unknown",
                      "id": "unknown", "last_updated": None, "ingested_at": None})

    nulls_after = sum(sdf.filter(F.col(c).isNull()).count() for c in numeric_cols)
    ops.append(f"Nulos numéricos imputados      : con mediana de cada columna")
    ops.append(f"Nulos de texto reemplazados    : con 'UNKNOWN' / 'Unknown'")
    ops.append(f"Nulos restantes tras limpieza  : {nulls_after}")
    print(f"[CLEAN] Nulos manejados (restantes: {nulls_after})")

    # 4.4 Normalización: columna price_change_pct_24h redondeada a 4 decimales
    sdf = sdf.withColumn("price_change_pct_24h", F.round("price_change_pct_24h", 4))
    ops.append("Normalización                  : price_change_pct_24h redondeado a 4 decimales")

    # 4.5 Columna adicional: categoría por market_cap_rank
    sdf = sdf.withColumn(
        "tier",
        F.when(F.col("market_cap_rank") <= 10,  F.lit("Top 10"))
         .when(F.col("market_cap_rank") <= 50,  F.lit("Top 50"))
         .otherwise(F.lit("Top 100"))
    )
    ops.append("Transformación adicional       : columna 'tier' (Top 10 / Top 50 / Top 100)")
    print("[CLEAN] Columna 'tier' agregada")

    # 4.6 Eliminar registros con price <= 0
    before = sdf.count()
    sdf = sdf.filter(F.col("current_price") > 0)
    removed_invalid = before - sdf.count()
    ops.append(f"Registros con precio <= 0 eliminados: {removed_invalid}")
    print(f"[CLEAN] Registros inválidos eliminados: {removed_invalid}")

    return sdf, ops

# ── 5. Exportar a Excel ───────────────────────────────────────────────────────
def export_excel(sdf):
    print("\n[EXPORT] Generando cleaned_data.xlsx...")
    pdf = sdf.orderBy("market_cap_rank").toPandas()
    pdf.to_excel(XLSX_PATH, index=False, engine="openpyxl")
    print(f"[EXPORT] Archivo guardado: {XLSX_PATH} ({len(pdf)} registros)")
    return pdf

# ── 6. Reporte de auditoría ───────────────────────────────────────────────────
def generate_report(stats_before: dict, records_after: int, ops: list, pdf_clean: pd.DataFrame):
    print("\n[REPORT] Generando cleaning_report.txt...")
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "=" * 65,
        "     REPORTE DE AUDITORÍA — PREPROCESAMIENTO EA2",
        "=" * 65,
        f"Fecha/Hora UTC      : {now}",
        f"Fuente              : src/db/ingestion.db (entorno cloud simulado)",
        f"Motor de procesamiento: PySpark (local) + Pandas",
        "",
        "── ESTADO INICIAL (antes de limpieza) ──────────────────────",
        f"  Registros totales  : {stats_before['total_records']}",
        f"  Columnas           : {stats_before['total_columns']}",
        f"  Duplicados         : {stats_before['duplicate_records']}",
        f"  Total nulos        : {stats_before['total_nulls']}",
        "",
        "  Nulos por columna:",
    ]
    for col, n in stats_before["nulls_per_column"].items():
        if n > 0:
            lines.append(f"    {col:<30}: {n}")
    if all(v == 0 for v in stats_before["nulls_per_column"].values()):
        lines.append("    (ninguna columna con nulos)")

    lines += [
        "",
        "── OPERACIONES REALIZADAS ───────────────────────────────────",
    ]
    for op in ops:
        lines.append(f"  ✔ {op}")

    lines += [
        "",
        "── ESTADO FINAL (después de limpieza) ──────────────────────",
        f"  Registros limpios  : {records_after}",
        f"  Columnas finales   : {len(pdf_clean.columns)}",
        f"  Reducción          : {stats_before['total_records'] - records_after} registros eliminados",
        "",
        "── DISTRIBUCIÓN POR TIER ────────────────────────────────────",
    ]
    tier_counts = pdf_clean["tier"].value_counts()
    for tier, count in tier_counts.items():
        lines.append(f"  {tier:<12}: {count} monedas")

    lines += [
        "",
        "── TOP 10 MONEDAS LIMPIAS ───────────────────────────────────",
    ]
    top10 = pdf_clean.sort_values("market_cap_rank").head(10)
    for _, row in top10.iterrows():
        lines.append(
            f"  #{int(row['market_cap_rank']):>3}  {row['name']:<20}"
            f"  ${row['current_price']:>14,.2f}  [{row['tier']}]"
        )

    lines += [
        "",
        "── ESTADÍSTICAS NUMÉRICAS (datos limpios) ───────────────────",
    ]
    numeric_summary = pdf_clean[["current_price", "market_cap", "total_volume",
                                  "price_change_pct_24h", "ath"]].describe()
    lines.append(numeric_summary.to_string())

    lines += ["", "=" * 65, "Fin del reporte.", "=" * 65]

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[REPORT] Reporte guardado: {REPORT_PATH}")

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🚀 EA2 - Preprocesamiento y Limpieza de Datos\n")

    # 1. Cargar datos
    pdf_raw = load_from_db()

    # 2. Iniciar Spark
    spark = create_spark()

    # 3. Convertir a Spark DataFrame
    sdf = spark.createDataFrame(pdf_raw)
    print(f"[SPARK] DataFrame creado con {sdf.count()} filas y {len(sdf.columns)} columnas")

    # 4. Análisis exploratorio
    stats = exploratory_analysis(sdf)

    # 5. Limpieza
    sdf_clean, operations = clean_data(sdf)

    # 6. Exportar Excel
    pdf_clean = export_excel(sdf_clean)

    # 7. Reporte
    generate_report(stats, len(pdf_clean), operations, pdf_clean)

    spark.stop()
    print("\n✅ Preprocesamiento completado.\n")
