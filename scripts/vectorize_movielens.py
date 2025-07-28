# vectorize_movielens_st.py
import numpy as np
import pandas as pd
from pathlib import Path
from sentence_transformers import SentenceTransformer
import pickle

def main():
    # 1. Rutas
    BASE_DIR = Path(__file__).resolve().parents[1]
    clean_path = BASE_DIR / "data" / "cleaned" / "movielens-clean.parquet"
    out_dir    = BASE_DIR / "data" / "vectorized"
    out_dir.mkdir(exist_ok=True)

    # 2. Cargar datos limpios
    df = pd.read_parquet(clean_path)

    # 3. Preparar corpus: título + géneros (puedes también incluir sinopsis)
    df["doc"] = df["title"] + " " + df["genres"].str.replace("|", " ")

    # 4. Cargar modelo preentrenado
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # 5. Generar embeddings (muestra barra de progreso)
    embeddings = model.encode(
        df["doc"].tolist(),
        show_progress_bar=True,
        convert_to_numpy=True
    )

    # 6. Guardar el modelo y los embeddings
    with open(out_dir / "st_model.pkl", "wb") as f:
        pickle.dump(model, f)
    np.save(out_dir / "movielens_st_embeds.npy", embeddings)

    print(f"[OK] Embeddings ST generados: {embeddings.shape[0]} ítems × {embeddings.shape[1]} dimensiones")

if __name__ == "__main__":
    main()
