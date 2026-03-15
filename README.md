# Proyecto Integrador Big Data — EA1 + EA2

Repositorio del proyecto integrador de **Big Data**.  
Fuente de datos: [CoinGecko API](https://www.coingecko.com/en/api) (pública, sin API key).

---

## 📦 Etapas del proyecto

| Etapa | Descripción | Script |
|---|---|---|
| **EA1** | Ingestión de datos desde API | `src/ingestion.py` |
| **EA2** | Preprocesamiento y limpieza | `src/cleaning.py` |

---

## 🗂 Estructura del proyecto

```
meza_bayron/
├── setup.py
├── README.md
├── .github/
│   └── workflows/
│       └── bigdata.yml           ← GitHub Actions (EA1 + EA2)
└── src/
    ├── ingestion.py              ← EA1: extracción desde API
    ├── cleaning.py               ← EA2: limpieza con PySpark + Pandas
    ├── db/
    │   └── ingestion.db          ← Base de datos SQLite
    ├── csv/
    │   ├── muestra.csv           ← EA1: top 20 monedas crudas
    │   └── cleaned_data.csv     ← EA2: datos limpios completos
    └── static/
        └── auditoria/
            ├── ingestion.txt     ← EA1: reporte de auditoría ingesta
            └── cleaning_report.txt ← EA2: reporte de limpieza
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
pip install requests pandas — pyspark
```
> PySpark requiere tener **Java 11 o 17** instalado. Descárgalo en https://adoptium.net

### 3. Ejecutar EA1 (ingesta)
```bash
python src/ingestion.py
```

### 4. Ejecutar EA2 (limpieza)
```bash
python src/cleaning.py
```

---

## EA1 — Ingestión de Datos

El script `src/ingestion.py` ejecuta 4 pasos:

1. **Llama a CoinGecko API** y descarga las top 100 criptomonedas con sus métricas.
2. **Guarda en SQLite** (`src/db/ingestion.db`) en la tabla `coins`.
3. **Genera un CSV** (`src/csv/muestra.csv`) con las primeras 20 monedas.
4. **Genera auditoría** (`src/static/auditoria/ingestion.txt`) comparando API vs BD.

---

## EA2 — Preprocesamiento y Limpieza

El script `src/cleaning.py` simula un entorno Big Data en la nube usando **PySpark en modo local** y ejecuta:

1. **Carga los datos** desde `ingestion.db` (simula almacenamiento cloud).
2. **Análisis exploratorio** con PySpark: detecta duplicados, nulos e inconsistencias.
3. **Limpieza y transformación:**
   - Eliminación de duplicados por `id`
   - Corrección de tipos de datos (cast numéricos, fechas a timestamp, símbolos a mayúsculas)
   - Imputación de nulos numéricos con la mediana de cada columna
   - Eliminación de registros con precio ≤ 0
   - Normalización de `price_change_pct_24h` a 4 decimales
   - Nueva columna `tier` (Top 10 / Top 50 / Top 100)
4. **Exporta** el DataFrame limpio a `src/csv/cleaned_data.csv`.
5. **Genera reporte** `src/static/auditoria/cleaning_report.txt` con estadísticas antes/después.

---

## 🗄️ Esquema de la base de datos — Tabla `coins`

| Columna | Tipo | Qué significa | Ejemplo |
|---|---|---|---|
| **id** | TEXT (PK) | Identificador único en CoinGecko | `bitcoin` |
| **symbol** | TEXT | Abreviatura (en mayúsculas tras limpieza) | `BTC` |
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

> **Columna adicional (EA2):** `tier` — categoriza cada moneda como `Top 10`, `Top 50` o `Top 100` según su `market_cap_rank`.

---

## 👁️ Cómo ver los archivos generados

### Base de datos SQLite
1. Descarga `src/db/ingestion.db`
2. Entra a 👉 **https://sqliteviewer.app** y arrastra el archivo

### Excel limpio
- Descarga `src/csv/cleaned_data.csv` y ábrelo con Excel o Google Sheets

### Reportes de auditoría
- `src/static/auditoria/ingestion.txt` — auditoría de ingesta (EA1)
- `src/static/auditoria/cleaning_report.txt` — auditoría de limpieza (EA2)

---

## 🤖 Automatización con GitHub Actions

El workflow `.github/workflows/bigdata.yml` se ejecuta:

- **En cada push** a la rama `master`
- **Diariamente a las 06:00 UTC**
- **Manualmente** desde Actions → Run workflow

### Pasos del workflow

| Paso | Etapa | Descripción |
|---|---|---|
| Checkout + Python | Ambas | Configura el entorno |
| Instalar dependencias EA1 | EA1 | requests, pandas, — |
| Ejecutar ingestion.py | EA1 | Extrae datos y llena la BD |
| Instalar Java + PySpark | EA2 | Prepara entorno Spark |
| Ejecutar cleaning.py | EA2 | Limpia y transforma los datos |
| Publicar artefactos | Ambas | Guarda los 5 archivos como descargables (30 días) |
| Commit automático | Ambas | Sube archivos actualizados al repo |

---

## 📦 Dependencias

| Librería | Uso |
|---|---|
| `requests` | Conexión HTTP a CoinGecko |
| `sqlite3` | Almacenamiento local (incluida en Python) |
| `pandas` | Exportación CSV/Excel y auditoría |
| `pyspark` | Procesamiento distribuido (simula entorno cloud) |
| `—` | Soporte formato Excel |
