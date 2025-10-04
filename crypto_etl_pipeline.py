from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests
import os
from pathlib import Path
import requests.exceptions

# --- IMPORTS Y CONFIGURACIÓN DE AIRFLOW ---

# La librería Airflow no es necesaria si solo se usa los decoradores:
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG

# --- LIBRERÍAS DE CALIDAD ---
from ydata_profiling import ProfileReport 

# --- VARIABLES GLOBALES Y RUTAS DE ARCHIVO ---
# NOTA: Se usa /tmp/ para los archivos intermedios, que es seguro para Airflow
TEMP_DIR = "/tmp/"

# Ruta del archivo original: debe apuntar a la carpeta 'include' del proyecto.
CSV_FILE_NAME = "CryptocurrencyData.csv"

# **CORRECCIÓN CLAVE** para que Airflow encuentre el archivo en 'include/'
# Utiliza la ruta absoluta del entorno de Airflow.
CSV_INPUT_PATH = str(
    Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
    / "include"
    / CSV_FILE_NAME
)

API_DATA_PATH = os.path.join(TEMP_DIR, "api_cryptocurrency_data.csv")
CLEANED_HISTORICAL_PATH = os.path.join(TEMP_DIR, "cleaned_cryptocurrency_data.csv")
FINAL_CLASSIFIED_PATH = os.path.join(TEMP_DIR, "final_classified_report.csv")


# =================================================================
# FASE 1: INGESTIÓN DE DATOS (T1)
# =================================================================


def coingecko_api():
    """
    T1.1: Extrae datos de la API de CoinGecko y retorna un DataFrame.
    (CÓDIGO GUARDADO)
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()  # Lanza una excepción para errores 4xx/5xx

        js = r.json()
        df = pd.DataFrame(js)

        columnas = [
            "symbol",
            "current_price",
            "price_change_percentage_24h",
            "market_cap",
            "total_volume",
            "high_24h",
            "low_24h",
        ]

        df = df[columnas]
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error en la API: {e}")
        return pd.DataFrame()


@task(task_id="ingest_api_data")
def ingest_api_data_task():
    """
    T1.2: Ejecuta la función de la API y guarda el resultado.
    (CÓDIGO GUARDADO - Modificado para usar la función)
    """
    df = coingecko_api()
    if not df.empty:
        df.to_csv(API_DATA_PATH, index=False)
    else:
        raise ValueError("Error al obtener datos de la API o DataFrame vacío.")


# =================================================================
# FASE 2: LIMPIEZA DE DATOS HISTÓRICOS (T2)
# =================================================================


def clean_total_supply(supply_str):
    """
    Función de limpieza avanzada para 'total_supply'.
    (CÓDIGO GUARDADO)
    """
    if pd.isna(supply_str) or supply_str == "-" or supply_str == "":
        return pd.NA

    supply_str = str(supply_str).replace(",", "").replace("$", "").strip()
    supply_str = supply_str.replace(" ", "")

    if "Billion" in supply_str:
        supply_str = supply_str.replace("Billion", "")
        try:
            return int(float(supply_str) * 1_000_000_000)
        except ValueError:
            return pd.NA
    elif "Million" in supply_str:
        supply_str = supply_str.replace("Million", "")
        try:
            return int(float(supply_str) * 1_000_000)
        except ValueError:
            return pd.NA
    else:
        try:
            return int(float(supply_str))
        except ValueError:
            return pd.NA


@task(task_id="clean_historical_data")
def clean_historical_data_task():
    """
    T2: Carga, limpia y guarda el DataFrame histórico.
    (COMBINACIÓN DE TODOS LOS CÓDIGOS DE LIMPIEZA GUARDADOS)
    """
    try:
        btcusd_df = pd.read_csv(CSV_INPUT_PATH)
    except FileNotFoundError as err:
        raise FileNotFoundError(
            f"Archivo CSV no encontrado en la ruta: {CSV_INPUT_PATH}"
        ) from err

    # 1. Limpieza de nombres de columna (CÓDIGO GUARDADO)
    btcusd_df.columns = btcusd_df.columns.str.strip()
    btcusd_df = btcusd_df.rename(
        columns={
            "Coin Name": "coin",
            "Symbol": "symbol",
            "Price": "current_price",
            "24h Volume": "24h_volume",
            "Circulating Supply": "circulating_supply",
            "Total Supply": "total_supply",
            "Market Cap": "market_cap",
        }
    )
    btcusd_df = btcusd_df.drop(columns=["Rank"])

    # 2. Limpieza de 'current_price' (CÓDIGO GUARDADO)
    btcusd_df["current_price"] = (
        btcusd_df["current_price"].astype(str).str.replace(",", "", regex=False)
    )
    btcusd_df["current_price"] = pd.to_numeric(
        btcusd_df["current_price"], errors="coerce"
    )

    # 3. Limpieza de columnas de porcentaje (CÓDIGO GUARDADO)
    percentage_cols = ["1h", "24h", "7d", "30d"]
    for col in percentage_cols:
        btcusd_df[col] = btcusd_df[col].astype(str).str.replace("%", "", regex=False)
        btcusd_df[col] = pd.to_numeric(btcusd_df[col], errors="coerce")
        btcusd_df[col] = btcusd_df[col] / 100

    # 4. Limpieza de Volume y Market Cap (CÓDIGO GUARDADO)
    volume_market_cols = ["24h_volume", "market_cap"]
    for col in volume_market_cols:
        btcusd_df[col] = (
            btcusd_df[col].astype(str).str.replace(r"[$, ]", "", regex=True)
        )
        btcusd_df[col] = pd.to_numeric(btcusd_df[col], errors="coerce")

    # 5. Limpieza de Circulating Supply (CÓDIGO GUARDADO)
    btcusd_df["circulating_supply"] = (
        btcusd_df["circulating_supply"].astype(str).str.replace(",", "", regex=False)
    )
    btcusd_df["circulating_supply"] = pd.to_numeric(
        btcusd_df["circulating_supply"], errors="coerce"
    )

    # 6. Limpieza avanzada de Total Supply (CÓDIGO GUARDADO)
    btcusd_df["total_supply"] = btcusd_df["total_supply"].apply(clean_total_supply)
    btcusd_df["total_supply"] = btcusd_df["total_supply"].astype("Int64")

    # 7. Detección de Outliers (CÓDIGO GUARDADO - Se mantiene la columna para el reporte final)
    q1 = btcusd_df["current_price"].quantile(0.25)
    q3 = btcusd_df["current_price"].quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    btcusd_df["is_outlier"] = ~btcusd_df["current_price"].between(lower, upper)

    # 8. Imputación de Valores Faltantes (CÓDIGO GUARDADO)
    for col in btcusd_df.select_dtypes(include=["float64", "Int64"]).columns:
        if btcusd_df[col].isnull().any():
            median_value = btcusd_df[col].median()
            btcusd_df[col] = btcusd_df[col].fillna(median_value)

    # 9. Guardar el DataFrame Limpio (CÓDIGO GUARDADO)
    btcusd_df.to_csv(CLEANED_HISTORICAL_PATH, index=False)

    print(f"Dataset guardado en {CLEANED_HISTORICAL_PATH}")


# =================================================================
# FASE 3: CLASIFICACIÓN DE LOS DATOS DE LA API (T3)
# =================================================================


def classify_trend(row):
    """
    Función para clasificar la tendencia.
    (Lógica propuesta previamente para Clasificación - Requisito 3)
    """
    price_change_api = row["price_change_percentage_24h"] / 100

    if price_change_api >= 0.05:
        return "TENDENCIA FUERTE ALCISTA"
    elif price_change_api > 0.01:
        return "TENDENCIA MODERADA ALCISTA"
    elif price_change_api <= -0.05:
        return "TENDENCIA FUERTE BAJISTA"
    elif price_change_api < -0.01:
        return "TENDENCIA MODERADA BAJISTA"
    else:
        return "TENDENCIA ESTABLE"


@task(task_id="classify_api_data")
def classify_api_data_task():
    """
    T3: Carga los datos de la API, aplica clasificación de tendencias y guarda el resultado final.
    """
    # Cargar únicamente datos de la API (T1)
    df_tiempo_real = pd.read_csv(API_DATA_PATH)

    # Clasificación (Etiquetado) aplicada a los datos de la API
    df_tiempo_real["trend_classification"] = df_tiempo_real.apply(
        classify_trend, axis=1
    )

    # Almacenamiento del Dataset Final Clasificado (solo API con clasificación)
    df_tiempo_real.to_csv(FINAL_CLASSIFIED_PATH, index=False)

    print(f"Clasificación completada. Dataset guardado en {FINAL_CLASSIFIED_PATH}")


# =================================================================
# FASE 4: DOCUMENTACIÓN (T4)
# =================================================================


@task(task_id="generate_data_quality_reports")
def generate_reports_task():
    """
    T4: Genera los dos Data Quality Reports solicitados.
    (CÓDIGOS GUARDADOS)
    """
    # --- 1. Reporte de Datos de API (CÓDIGO GUARDADO)
    try:
        df_api = pd.read_csv(API_DATA_PATH)
        profile_api = ProfileReport(
            df_api, title="Reporte de Calidad - Criptomonedas API"
        )
        profile_api.to_file(os.path.join(BASE_PATH, "cryptos_api_report.html"))
        print("Reporte de API generado.")
    except Exception as e:
        print(f"Error al generar reporte de API: {e}")

    # --- 2. Reporte del Dataset Final Clasificado (Usando modo mínimo para eficiencia)
    try:
        df_final = pd.read_csv(FINAL_CLASSIFIED_PATH)
        profile_final = ProfileReport(
            df_final, title="Reporte de Calidad - FINAL Clasificado", minimal=True
        )  # Modo Mínimo (CÓDIGO GUARDADO)
        profile_final.to_file(os.path.join(BASE_PATH, "cryptos_final_report.html"))
        print("Reporte Final (Mínimo) generado.")
    except Exception as e:
        print(f"Error al generar reporte Final: {e}")


# =================================================================
# FASE 5: DESCARGA DE DATOS (T5)
# =================================================================


@task(task_id="download_final_data")
def download_final_data_task():
    """
    T5: Carga/guarda AMBOS datasets procesados en una ubicación persistente: 'include/'.
    - Dataset histórico limpio (de Kaggle procesado)
    - Dataset de API con clasificación de tendencias
    """
    import shutil

    # Verificar que ambos archivos finales existen
    if not os.path.exists(FINAL_CLASSIFIED_PATH):
        raise FileNotFoundError(
            f"Archivo de API clasificado no encontrado en: {FINAL_CLASSIFIED_PATH}"
        )

    if not os.path.exists(CLEANED_HISTORICAL_PATH):
        raise FileNotFoundError(
            f"Archivo histórico limpio no encontrado en: {CLEANED_HISTORICAL_PATH}"
        )

    # Definir la ruta de descarga - usando la carpeta include del proyecto que es accesible
    # Esta ruta apunta a la carpeta include de tu proyecto Astro (donde está CryptocurrencyData.csv)
    PERSISTENT_OUTPUT_DIR = str(
        Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")) / "include"
    )
    os.makedirs(PERSISTENT_OUTPUT_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Rutas para ambos datasets
    api_download_path = os.path.join(
        PERSISTENT_OUTPUT_DIR,
        f"crypto_api_classified_{timestamp}.csv",
    )

    historical_download_path = os.path.join(
        PERSISTENT_OUTPUT_DIR,
        f"crypto_historical_cleaned_{timestamp}.csv",
    )

    # Copiar ambos archivos a la carpeta de descargas
    shutil.copy2(FINAL_CLASSIFIED_PATH, api_download_path)
    shutil.copy2(CLEANED_HISTORICAL_PATH, historical_download_path)

    # Leer ambos archivos para mostrar información básica
    df_api = pd.read_csv(api_download_path)
    df_historical = pd.read_csv(historical_download_path)

    print("✅ AMBOS datasets cargados exitosamente en ubicaciones persistentes:")
    print(f"📊 Dataset API con Clasificación: {api_download_path}")
    print(f"   - Filas: {len(df_api)}")
    print(f"   - Columnas: {len(df_api.columns)}")
    print(f"   - Columnas incluyen: {list(df_api.columns)}")
    print(f"   - Tamaño: {os.path.getsize(api_download_path)} bytes")

    print(f"📊 Dataset Histórico Limpio: {historical_download_path}")
    print(f"   - Filas: {len(df_historical)}")
    print(f"   - Columnas: {len(df_historical.columns)}")
    print(f"   - Columnas incluyen: {list(df_historical.columns)}")
    print(f"   - Tamaño: {os.path.getsize(historical_download_path)} bytes")

    return {
        "api_classified": api_download_path,
        "historical_cleaned": historical_download_path,
    }


# =================================================================
# DEFINICIÓN DEL DAG Y DEPENDENCIAS
# =================================================================


@dag(
    dag_id="crypto_data_pipeline_final_project",
    start_date=datetime(2025, 9, 30),
    schedule="0 */6 * * *",  # Cada 6 horas (4 veces al día)
    catchup=False,
    tags=["etl_final", "criptomonedas", "clasificacion"],
)
def crypto_classification_dag():
    # Instanciación de las Tareas
    t1 = ingest_api_data_task()
    t2 = (
        clean_historical_data_task()
    )  # Se mantiene para procesamiento histórico opcional
    t3 = classify_api_data_task()
    t4 = generate_reports_task()
    t5 = download_final_data_task()

    # Definición de las Dependencias (Flujo del Pipeline)
    # T1 obtiene datos de API, T2 procesa datos históricos independientemente
    # T3 solo depende de T1 (datos API) para clasificación
    # T4 y T5 esperan a T3 y se ejecutan en paralelo
    [t1, t2] >> t3 >> [t4, t5]


# Instanciar el DAG
crypto_classification_pipeline = crypto_classification_dag()

