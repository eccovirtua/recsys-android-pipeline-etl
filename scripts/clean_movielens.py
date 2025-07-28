import pandas as pd
from pathlib import Path

# 1. Determinar la raíz del proyecto
BASE_DIR = Path(__file__).resolve().parents[1]
print(f"BASE_DIR={BASE_DIR}")

# 2. Ajustar rutas al directorio `ml-latest-small`
data_folder = BASE_DIR / "data" / "movielens" / "ml-latest-small"
ratings_path = data_folder / "ratings.csv"
movies_path  = data_folder / "movies.csv"

# 3. Verificar existencia
for p in (ratings_path, movies_path):
    if not p.exists():
        raise FileNotFoundError(f"No existe: {p}")

# 4. Leer CSVs
ratings = pd.read_csv(ratings_path)
movies  = pd.read_csv(movies_path)

# 5. Limpiar duplicados
ratings.drop_duplicates(subset=['userId','movieId'], inplace=True)

# 6. Convertir timestamp a datetime
ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

# 7. Merge para metadatos
df = ratings.merge(movies, how='left', on='movieId')

# 8. Seleccionar columnas esenciales
df_clean = df[['userId','movieId','title','genres','rating','timestamp']]

# 9. Crear carpeta de salida
out_dir = BASE_DIR / "data" / "cleaned"
out_dir.mkdir(parents=True, exist_ok=True)

# 10. Guardar en Parquet
out_path = out_dir / "movielens-clean.parquet"
df_clean.to_parquet(out_path, index=False)

print(f"Dataset limpio guardado en: {out_path}")
