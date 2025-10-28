import json
import logging
from pathlib import Path
from typing import List, Sequence
import pendulum
import requests
import pandas as pd

logger = logging.getLogger(__name__)

countries = ["MEX", "BRA", "ARG", "USA", "COL","BOL","CAN","CHL","ECU","PER","URY"]
#indicators = {
#    "NY.GDP.MKTP.CD": "PIB (USD actuales)",
#    "SP.POP.TOTL": "Población total",
#    "SP.POP.TOTL.FE.IN": "Población femenina",
#    "SP.POP.TOTL.MA.IN": "Población masculina",
#    "EN.GHG.CO2.AG.MT.CE.AR5": "Emisiones de dióxido de carbono (CO2) de la agricultura",    
#}
indicators = {
    "NY.GDP.MKTP.CD": "PIB (USD actuales)",
    "SP.POP.TOTL": "Población total",
    "SP.POP.TOTL.FE.IN": "Población femenina",
    "SP.POP.TOTL.MA.IN": "Población masculina",
}
#start_year = 2020
#end_year = 2023

def get_indicator_data(country, indicator, start_year, end_year):
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=1000&date={start_year}:{end_year}"
    r = requests.get(url)
    #data = r.json()
    return r

# start_date: pendulum.Date,
def ingest_to_raw(    
    output_dir: Path | str,
    start_year: int,
    end_year: int
) -> Sequence[str]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_files: List[str] = []
        
    for c in countries:
        for i in indicators.keys():
            results = get_indicator_data(c, i, start_year, end_year)
              
            daily_output_dir = (
                output_dir / f"{c}" / f"{i}" 
            )
            daily_output_dir.mkdir(parents=True, exist_ok=True)

            file_path = daily_output_dir / "datos_abiertos.json"
            file_path.write_text(json.dumps(results.json(), indent=2), encoding="utf-8")
            saved_files.append(str(file_path))

    return saved_files


