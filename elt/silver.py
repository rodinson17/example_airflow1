from pathlib import Path


def _clean_column_name(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
    )


def transform_bronze_to_silver(
    bronze_path: Path | str,
    silver_path: Path | str,
) -> str:
    import pandas as pd
    import numpy as np

    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    df.columns = [_clean_column_name(col) for col in df.columns]

    rename_map = {  
        "date": "year",        
        "indicator_id": "codigo_indicador",
        "indicator_value": "descripcion_indicador",
        "country_id": "codigo_pais",
        "countryiso3code": "codigo_pais_iso",
        "country_value": "descripcion_pais",       
        "value": "valor",   
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        
    df = df.drop_duplicates()

    df = df.drop(columns=['unit','obs_status','decimal'])   

    df["descripcion_indicador"] = df["descripcion_indicador"].str.upper()
    df["descripcion_pais"] = df["descripcion_pais"].str.upper()

    #df['valor'] = df['valor'].replace({'null':0})
    #df['valor'] = df['valor'].fillna('0', inplace=True)

    #print(f"datos nulos de: {df['valor'].isnull().sum}")
    #print(f"datos nan de: {df['valor'].isna().sum}")
    #print(f"informacion de: {df['valor'].info()}")
    print(df['valor'].isna())

    df.to_parquet(silver_path, index=False)
    return str(silver_path)