# Proyecto Integrador Big Data — EA1 + EA2 + EA3

Repositorio del proyecto integrador de **Big Data**.  
Fuente de datos principal: [CoinGecko API](https://www.coingecko.com/en/api) (pública, sin API key).

---

## 📦 Etapas del proyecto

| Etapa | Descripción | Script |
|---|---|---|
| **EA1** | Ingestión de datos desde API | `src/ingestion.py` |
| **EA2** | Preprocesamiento y limpieza | `src/cleaning.py` |
| **EA3** | Enriquecimiento de datos | `src/enrichment.py` |

---

## 🗂 Estructura del proyecto

```
meza_bayron/
├── setup.py
├── README.md
├── .github/
│   └── workflows/
│       └── bigdata.yml              ← GitHub Actions (EA1 + EA2 + EA3)
└── src/
    ├── ingestion.py                 ← EA1: extracción desde API
    ├── cleaning.py                  ← EA2: limpieza con PySpark + Pandas
    ├── enrichment.py                ← EA3: enriquecimiento con múltiples fuentes
    ├── data_sources/                ← Fuentes adicionales para EA3
    │   ├── crypto_categories.json   ← Categorías y sectores (JSON)
    │   ├── risk_metrics.csv         ← Métricas de riesgo (CSV)
    │   ├── regulatory_info.xml      ← Información regulatoria (XML)
    │   ├── sentiment_data.txt       ← Sentimiento de mercado (TXT)
    │   ├── project_info.xlsx        ← Info del proyecto/equipo (XLSX)
    │   └── exchange_listings.html   ← Listados en exchanges (HTML)
    ├── db/
    │   └── ingestion.db             ← Base de datos SQLite
    ├── xlsx/
    │   ├── muestra.csv              ← EA1: top 20 monedas crudas
    │   ├── cleaned_data.csv         ← EA2: datos limpios completos
    │   └── enriched_data.csv        ← EA3: dataset enriquecido final
    └── static/
        └── auditoria/
            ├── ingestion.txt        ← EA1: reporte de auditoría ingesta
            ├── cleaning_report.txt  ← EA2: reporte de limpieza
            └── enrichment_report.txt← EA3: reporte de enriquecimiento
```

---

## ⚙️ Instalación y ejecución local

### 1. Clonar el repositorio
```bash
git clone https://github.com/BayronMgDev/Datos-desde-un-API.git
cd Datos-desde-un-API
```

### 2. Instalar dependencias
```bash
pip install requests pandas openpyxl pyspark lxml
```
> PySpark requiere **Java 11 o 17**. Descárgalo en https://adoptium.net

### 3. Ejecutar en orden
```bash
python src/ingestion.py    # EA1
python src/cleaning.py     # EA2
python src/enrichment.py   # EA3
```

---

## EA1 — Ingestión de Datos

El script `src/ingestion.py` ejecuta 4 pasos:

1. **Llama a CoinGecko API** y descarga las top 100 criptomonedas con sus métricas.
2. **Guarda en SQLite** (`src/db/ingestion.db`) en la tabla `coins`.
3. **Genera un CSV** (`src/xlsx/muestra.csv`) con las primeras 20 monedas.
4. **Genera auditoría** (`src/static/auditoria/ingestion.txt`) comparando API vs BD.

---

## EA2 — Preprocesamiento y Limpieza

El script `src/cleaning.py` simula un entorno Big Data en la nube con **PySpark local**:

1. **Carga los datos** desde `ingestion.db`.
2. **Análisis exploratorio**: detecta duplicados, nulos e inconsistencias.
3. **Limpieza y transformación:**
   - Eliminación de duplicados por `id`
   - Corrección de tipos (cast numéricos, fechas a timestamp, símbolos a mayúsculas)
   - Imputación de nulos numéricos con la mediana
   - Eliminación de registros con precio ≤ 0
   - Normalización de `price_change_pct_24h` a 4 decimales
   - Nueva columna `tier` (Top 10 / Top 50 / Top 100)
4. **Exporta** el DataFrame limpio a `src/xlsx/cleaned_data.csv`.
5. **Genera reporte** `src/static/auditoria/cleaning_report.txt`.

---

## EA3 — Enriquecimiento de Datos

El script `src/enrichment.py` integra **6 fuentes adicionales** al dataset limpio:

| Fuente | Formato | Columnas añadidas |
|---|---|---|
| `crypto_categories.json` | JSON | category, sector, consensus, founded_year |
| `risk_metrics.csv` | CSV | risk_score, volatility_30d, sharpe_ratio, beta |
| `regulatory_info.xml` | XML | legal_status, regulated, main_exchange |
| `sentiment_data.txt` | TXT | sentiment_score, sentiment_label, social_mentions_24h |
| `project_info.xlsx` | XLSX | whitepaper_year, team_size, github_commits_30d, partnerships_count |
| `exchange_listings.html` | HTML | listed_exchanges, trading_pairs, listing_year, cmc_rank_2023 |

Además calcula la columna derivada `investment_score` combinando riesgo, sentimiento, actividad en GitHub y número de partnerships.

Genera:
- `src/xlsx/enriched_data.csv` — dataset enriquecido completo
- `src/static/auditoria/enrichment_report.txt` — reporte de integración

---

## 🗄️ Esquema de la base de datos — Tabla `coins`

| Columna | Tipo | Qué significa | Ejemplo |
|---|---|---|---|
| **id** | TEXT (PK) | Identificador único en CoinGecko | `bitcoin` |
| **symbol** | TEXT | Abreviatura (mayúsculas tras limpieza) | `BTC` |
| **name** | TEXT | Nombre completo | `Bitcoin` |
| **current_price** | REAL | Precio actual en USD | `83500.00` |
| **market_cap** | REAL | Capitalización de mercado | `1,650,000,000,000` |
| **market_cap_rank** | INTEGER | Ranking mundial por capitalización | `1` |
| **total_volume** | REAL | Volumen negociado en 24h | `45,000,000,000` |
| **high_24h** | REAL | Precio más alto en las últimas 24h | `84,200.00` |
| **low_24h** | REAL | Precio más bajo en las últimas 24h | `81,900.00` |
| **price_change_24h** | REAL | Cambio de precio en USD en 24h | `+1,200.00` |
| **price_change_pct_24h** | REAL | Cambio en porcentaje en 24h | `+1.4500` |
| **circulating_supply** | REAL | Monedas en circulación actualmente | `19,600,000` |
| **total_supply** | REAL | Cantidad máxima de monedas que existirán | `21,000,000` |
| **ath** | REAL | All Time High: precio histórico más alto | `108,786.00` |
| **last_updated** | TEXT | Fecha/hora de actualización en CoinGecko | `2026-03-01T10:00:00Z` |
| **ingested_at** | TEXT | Fecha/hora en que el script guardó el dato | `2026-03-01T06:00:00Z` |

> **EA2 añade:** `tier` (Top 10 / Top 50 / Top 100)
> **EA3 añade:** 17 columnas de las 6 fuentes + `investment_score`

---

## 👁️ Cómo ver los archivos generados

### Base de datos SQLite
1. Descarga `src/db/ingestion.db`
2. Entra a 👉 **https://sqliteviewer.app** y arrastra el archivo

### Archivos CSV (previsualización directa en GitHub)
- `src/xlsx/muestra.csv` — top 20 monedas crudas
- `src/xlsx/cleaned_data.csv` — datos limpios (EA2)
- `src/xlsx/enriched_data.csv` — datos enriquecidos (EA3)

### Reportes de auditoría
- `src/static/auditoria/ingestion.txt` — EA1
- `src/static/auditoria/cleaning_report.txt` — EA2
- `src/static/auditoria/enrichment_report.txt` — EA3

---

## 🤖 Automatización con GitHub Actions

El workflow `.github/workflows/bigdata.yml` se ejecuta:

- **En cada push** a la rama `master`
- **Diariamente a las 06:00 UTC**
- **Manualmente** desde Actions → Run workflow

### Pasos del workflow

| Paso | Etapa | Descripción |
|---|---|---|
| Checkout + Python | Todas | Configura el entorno |
| Instalar deps EA1 | EA1 | requests, pandas, openpyxl, lxml |
| Ejecutar ingestion.py | EA1 | Extrae datos y llena la BD |
| Instalar Java + PySpark | EA2 | Prepara entorno Spark |
| Ejecutar cleaning.py | EA2 | Limpia y transforma los datos |
| Ejecutar enrichment.py | EA3 | Enriquece con 6 fuentes adicionales |
| Publicar artefactos | Todas | Guarda los 7 archivos como descargables (30 días) |
| Commit automático | Todas | Sube archivos actualizados al repo |

---

## 📦 Dependencias

| Librería | Uso |
|---|---|
| `requests` | Conexión HTTP a CoinGecko |
| `sqlite3` | Almacenamiento local (incluida en Python) |
| `pandas` | Exportación CSV y operaciones de merge |
| `pyspark` | Procesamiento distribuido (simula entorno cloud) |
| `openpyxl` | Lectura/escritura de archivos Excel |
| `lxml` | Soporte para lectura de HTML con pandas |
