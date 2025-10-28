from pathlib import Path


def build_fact_incators(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    print(f"datos del df: {df}")

    columns = ["codigo_pais_iso", "year","valor","codigo_indicador"]
    available = [col for col in columns if col in df.columns]
    fact = df[available]

    print(f"datos del dim: {fact}")
    fact.to_parquet(output_path, index=False)
    return str(output_path)