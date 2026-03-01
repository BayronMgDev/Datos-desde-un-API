# EA1 — Ingestión de Datos desde un API

Proyecto integrador de **Big Data** — Etapa 1: Ingesta.  
Fuente de datos: [CoinGecko API](https://www.coingecko.com/en/api) (pública, sin API key).

---

## ¿Qué hace el proyecto?

El script `src/ingestion.py` ejecuta 4 pasos en orden:

1. **Llama a CoinGecko API** y descarga las top 100 criptomonedas (Bitcoin, Ethereum, etc.) con sus métricas de mercado.
2. **Guarda todo en SQLite** (`src/db/ingestion.db`) en una tabla llamada `coins`. Si ya existe un registro, lo actualiza con los precios más recientes.
3. **Genera un CSV** (`src/xlsx/muestra.csv`) con las primeras 20 monedas ordenadas por ranking de mercado.
4. **Genera un reporte de auditoría** (`src/static/auditoria/ingestion.txt`) que compara cuántos registros trajo el API vs cuántos quedaron en la BD.

---

## Estructura del proyecto

```
meza_bayron/
├── setup.py
├── README.md
├── .github/
│   └── workflows/
│       └── bigdata.yml          ← GitHub Actions (automatización)
└── src/
    ├── ingestion.py             ← Script principal
    ├── db/
    │   └── ingestion.db         ← Base de datos SQLite
    ├── xlsx/
    │   └── muestra.csv          ← Muestra top 20 monedas
    └── static/
        └── auditoria/
            └── ingestion.txt    ← Reporte de auditoría
```

---

## Instalación y ejecución local

### 1. Clonar el repositorio
```bash
git clone https://github.com/BayronMgDev/Datos-desde-un-API.git
cd Datos-desde-un-API
```

### 2. Crear entorno virtual e instalar dependencias
```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install requests pandas openpyxl
```

### 3. Ejecutar el script
```bash
python src/ingestion.py
```

Al terminar encontrarás los tres archivos de evidencia en sus respectivas carpetas.

---

## Esquema de la base de datos

### Tabla `coins`

| Columna | Tipo | Qué significa | Ejemplo |
|---|---|---|---|
| **id** | TEXT (PK) | Identificador único de la moneda en CoinGecko | `bitcoin`, `ethereum` |
| **symbol** | TEXT | Abreviatura de la moneda | `btc`, `eth` |
| **name** | TEXT | Nombre completo de la moneda | `Bitcoin`, `Ethereum` |
| **current_price** | REAL | Precio actual en dólares (USD) | `83500.00` |
| **market_cap** | REAL | Capitalización = precio × monedas en circulación | `1,650,000,000,000` |
| **market_cap_rank** | INTEGER | Posición en el ranking mundial por capitalización | `1` = Bitcoin |
| **total_volume** | REAL | Dinero total negociado en las últimas 24 horas | `45,000,000,000` |
| **high_24h** | REAL | Precio más alto en las últimas 24h | `84,200.00` |
| **low_24h** | REAL | Precio más bajo en las últimas 24h | `81,900.00` |
| **price_change_24h** | REAL | Cambio de precio en dólares en 24h | `+1,200.00` |
| **price_change_pct_24h** | REAL | Cambio de precio en porcentaje en 24h | `+1.45` |
| **circulating_supply** | REAL | Cantidad de monedas en circulación actualmente | `19,600,000` |
| **total_supply** | REAL | Cantidad máxima de monedas que existirán | `21,000,000` |
| **ath** | REAL | All Time High: precio histórico más alto | `108,786.00` |
| **last_updated** | TEXT | Fecha/hora en que CoinGecko actualizó el dato | `2026-03-01T10:00:00Z` |
| **ingested_at** | TEXT | Fecha/hora en que el script guardó el registro | `2026-03-01T06:00:00Z` |

> **Diferencia clave:** `last_updated` es cuándo CoinGecko actualizó el precio. `ingested_at` es cuándo nuestro script lo descargó y guardó. Esto permite auditar el retraso entre la fuente y la base de datos.

### Sentencia SQL de creación
```sql
CREATE TABLE IF NOT EXISTS coins (
    id                   TEXT PRIMARY KEY,
    symbol               TEXT,
    name                 TEXT,
    current_price        REAL,
    market_cap           REAL,
    market_cap_rank      INTEGER,
    total_volume         REAL,
    high_24h             REAL,
    low_24h              REAL,
    price_change_24h     REAL,
    price_change_pct_24h REAL,
    circulating_supply   REAL,
    total_supply         REAL,
    ath                  REAL,
    last_updated         TEXT,
    ingested_at          TEXT
);
```

---

## Cómo ver la base de datos

Online (sin instalar nada) 
1. Descarga `ingestion.db` desde GitHub: `src/db/ingestion.db` → **Download raw file**
2. Entra a **https://sqliteviewer.app**
3. Arrastra el archivo `ingestion.db` a la página
4. Selecciona la tabla `coins` y verás los 100 registros


---

## Cómo descargar las evidencias

### Desde GitHub Actions (artefactos)
1. Ve a la pestaña **Actions** del repositorio
2. Haz clic en el último run exitoso
3. Baja hasta la sección **Artifacts**
4. Descarga el ZIP `evidencias-ingesta-N`

El ZIP contiene:
- `ingestion.db` — base de datos SQLite
- `muestra.csv` — top 20 monedas (abrir con Excel)
- `ingestion.txt` — reporte de auditoría

### Desde el repositorio directamente
Navega a cada archivo en la pestaña **Code** y haz clic en **Download raw file**.

---

## Automatización con GitHub Actions

El workflow `.github/workflows/bigdata.yml` se ejecuta:

- **En cada push** a la rama `master`
- **Diariamente a las 06:00 UTC** (cron schedule)
- **Manualmente** desde Actions → **Run workflow**

### Pasos del workflow

| Paso | Descripción |
|---|---|
| Checkout código | Descarga el repositorio |
| Configurar Python 3.11 | Prepara el entorno |
| Instalar dependencias | Instala requests, pandas, openpyxl |
| Ejecutar ingestion.py | Corre el script completo |
| Verificar archivos | Confirma que se generaron los 3 archivos |
| Publicar artefactos | Guarda los archivos como descargables (30 días) |
| Commit automático | Sube los archivos actualizados al repositorio |

### Verificar una ejecución
1. Ve a la pestaña **Actions**
2. Selecciona el workflow *BigData - Ingesta CoinGecko*
3. Abre cualquier run y revisa los logs de cada paso
4. Descarga los artefactos al final del run

---

## Dependencias

| Librería | Uso |
|---|---|
| `requests` | Conexión HTTP a la API de CoinGecko |
| `sqlite3` | Almacenamiento local (incluida en Python) |
| `pandas` | Generación del CSV de muestra y lectura para auditoría |
| `openpyxl` | Soporte formato Excel (opcional) |
