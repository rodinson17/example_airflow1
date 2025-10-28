import json
from pathlib import Path

import pandas as pd

def copy_raw_to_bronze(
    raw_root: Path | str,
    bronze_path: Path | str,
) -> str:
    raw_root = Path(raw_root)
    bronze_path = Path(bronze_path)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    
    datos_files = sorted(raw_root.glob("**/datos_abiertos.json"))
    records: list[dict] = []
    #print(f"carpetas: {datos_files}")
    for json_file in datos_files:
        payload = json.loads(json_file.read_text(encoding="utf-8"))
        if isinstance(payload[1], list):
            records.extend(payload[1])
        else:
            records.append(payload[1])

    #print(f"datos: {records}")
    df = pd.json_normalize(records) if records else pd.DataFrame()
    #df['value'] = df['value'].fillna(0, inplace=True)

    #print(df['value'].isna())
    df.to_parquet(bronze_path, index=False)
    return str(bronze_path)

