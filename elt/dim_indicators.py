from pathlib import Path


def build_dim_indicators(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    print(f"datos del df: {df['codigo_indicador']}")

    columns = ["codigo_indicador", "descripcion_indicador"]
    available = [col for col in columns if col in df.columns]
    dim = df[available].drop_duplicates(subset=["codigo_indicador"])

    print(f"datos del dim: {dim}")
    dim.to_parquet(output_path, index=False)
    return str(output_path)