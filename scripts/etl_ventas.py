import pandas as pd
import json
import os
import logging
from datetime import datetime
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient

# 1. CONFIGURACIÓN Y LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[logging.FileHandler("etl_ventas.log"), logging.StreamHandler()]
)
logger = logging.getLogger("ETL_Global")

NODOS = {
    "ARG": "postgresql://user_admin:password123@127.0.0.1:5433/ventas",
    "BRA": "postgresql://user_admin:password123@127.0.0.1:5434/ventas",
    "MEX": "postgresql://user_admin:password123@127.0.0.1:5435/ventas"
}

STATE_FILE = "etl_state.json"

CONTAINER_NAME = "datawarehouse1"  

AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# 2. GESTIÓN DE ESTADO (HWM)
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

# 3. NÚCLEO DEL ETL
def run_etl():
    state = load_state()
    all_data = []
    
    logger.info("Iniciando proceso de extracción incremental...")

    for pais, conn_str in NODOS.items():
        hwm = state.get(pais, "1900-01-01 00:00:00")
        try:
            engine = create_engine(conn_str)
            query = f"SELECT * FROM ventas WHERE ultima_actualizacion > '{hwm}'"
            
            # Extracción eficiente
            df = pd.read_sql(query, engine)
            
            if not df.empty:
                df['pais_origen'] = pais
                all_data.append(df)
                
                # Actualizar estado localmente
                state[pais] = str(df['ultima_actualizacion'].max())
                logger.info(f"{pais}: Extrayendo {len(df)} registros nuevos.")
            else:
                logger.info(f"{pais}: Sin datos nuevos desde {hwm}.")
                
        except Exception as e:
            logger.error(f"Fallo en la extracción de {pais}: {e}")

    if all_data:
        # Consolidación
        df_final = pd.concat(all_data, ignore_index=True)
        temp_file = "processed_data.parquet"
        df_final.to_parquet(temp_file, index=False)
        
        # Carga a la Nube
        if upload_to_azure(temp_file):
            save_state(state) # Solo guardamos el estado si la subida fue exitosa
            logger.info("Proceso completado y estado actualizado.")
    else:
        logger.info("No se generaron archivos nuevos. Nada que subir.")

def upload_to_azure(file_path):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
        blob_name = f"ventas/año={datetime.now().year}/mes={datetime.now().month}/ventas_delta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        logger.info(f"Archivo subido a Azure: {blob_name}")
        return True
    except Exception as e:
        logger.error(f"Error subiendo a Azure: {e}")
        return False

if __name__ == "__main__":
    run_etl()