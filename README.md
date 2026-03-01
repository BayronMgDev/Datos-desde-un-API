# EA1 — Ingestión de Datos desde un API

Proyecto integrador de **Big Data** — Etapa 1: Ingesta.  
Fuente de datos: [CoinGecko API](https://www.coingecko.com/en/api) (pública, sin API key).

---

## 📋 Descripción

El script extrae las **top 100 criptomonedas por capitalización de mercado** desde CoinGecko,
las almacena en una base de datos SQLite y genera dos archivos de evidencia:

| Archivo | Ruta | Descripción |
|---|---|---|
| Base de datos | `src/db/ingestion.db` | Tabla `coins` con métricas de mercado |
| Muestra CSV | `src/xlsx/muestra.csv` | Top 20 monedas exportadas con Pandas |
| Auditoría | `src/static/auditoria/ingestion.txt` | Comparación API vs BD |

---

## 🗂 Estructura del proyecto

```
nombre_apellido/
├── setup.py
├── README.md
├── .github/
│   └── workflows/
│       └── bigdata.yml          ← GitHub Actions
└── src/
    ├── ingestion.py             ← Script principal
    ├── static/
    │   └── auditoria/
    │       └── ingestion.txt
    ├── db/
    │   └── ingestion.db
    └── xlsx/
        └── muestra.csv
```

---

## ⚙️ Instalación y ejecución local

### 1. Clonar el repositorio
```bash
git clone https://github.com/<tu-usuario>/<nombre-repo>.git
cd <nombre-repo>
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

## 🤖 Automatización con GitHub Actions

El workflow `.github/workflows/bigdata.yml` se ejecuta:

- **En cada push** a la rama `main`
- **Diariamente a las 06:00 UTC** (cron schedule)
- **Manualmente** desde la pestaña *Actions* → *Run workflow*

### Pasos del workflow

1. Checkout del repositorio
2. Configurar Python 3.11
3. Instalar dependencias (`requests`, `pandas`, `openpyxl`)
4. Ejecutar `src/ingestion.py`
5. Verificar existencia y contenido de los archivos generados
6. Publicar los archivos como **artefactos descargables** (30 días de retención)
7. Hacer commit automático de los archivos actualizados al repositorio

### Verificar la ejecución

1. Ve a la pestaña **Actions** de tu repositorio.
2. Selecciona el workflow *BigData - Ingesta CoinGecko*.
3. Abre el último run y revisa los logs de cada step.
4. Descarga los artefactos desde la sección *Artifacts* al final del run.

---

## 🗄️ Esquema de la base de datos

```sql
CREATE TABLE coins (
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

## 📦 Dependencias

| Librería | Uso |
|---|---|
| `requests` | Conexión HTTP a la API de CoinGecko |
| `sqlite3` | Almacenamiento local (incluida en Python) |
| `pandas` | Generación del CSV de muestra y auditoría |
| `openpyxl` | Soporte Excel (opcional) |
