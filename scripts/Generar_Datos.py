import pandas as pd
import json
import os
from sqlalchemy import create_engine

# Configuración
NODOS = {
    "ARG": "postgresql://user_admin:password123@127.0.0.1:5433/ventas",
    "BRA": "postgresql://user_admin:password123@127.0.0.1:5434/ventas",
    "MEX": "postgresql://user_admin:password123@127.0.0.1:5435/ventas"
}
STATE_FILE = "et_state.json"

def get_last_hwm(pais):
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f).get(pais, "1900-01-01 00:00:00")
    return "1900-01-01 00:00:00"

def save_hwm(pais, timestamp):
    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
    state[pais] = str(timestamp)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def extract():
    all_deltas = []
    
    for pais, conn_str in NODOS.items():
        hwm = get_last_hwm(pais)
        print(f"[{pais}] Extrayendo deltas desde {hwm}...")
        
        engine = create_engine(conn_str)
        query = f"SELECT * FROM ventas WHERE ultima_actualizacion > '{hwm}'"
        
        df = pd.read_sql(query, engine)
        
        if not df.empty:
            df['pais_origen'] = pais
            all_deltas.append(df)
            # El nuevo HWM es la fecha más reciente del lote extraído
            new_hwm = df['ultima_actualizacion'].max()
            save_hwm(pais, new_hwm)
            print(f"[{pais}] Se encontraron {len(df)} registros nuevos.")
        else:
            print(f"[{pais}] Sin novedades.")

    if all_deltas:
        df_final = pd.concat(all_deltas, ignore_index=True)
        # Guardamos en Parquet para eficiencia en Cloud y Power BI
        df_final.to_parquet("ventas_delta.parquet", index=False)
        return True
    return False

if __name__ == "__main__":
    extract()