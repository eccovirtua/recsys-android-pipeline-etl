# scripts/etl_flow.py
import sys
from pathlib import Path
import subprocess

from prefect import flow, task

@task(retries=2, retry_delay_seconds=10)
def clean_movielens_task():
    """
    Ejecuta el script de limpieza de MovieLens usando el mismo intérprete.
    """
    base_dir = Path(__file__).resolve().parents[1]
    script_path = base_dir / "scripts" / "clean_movielens.py"

    # Usa sys.executable (la ruta al Python de tu venv)
    cmd = [sys.executable, str(script_path)]
    result = subprocess.run(
        cmd,
        cwd=str(base_dir),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Error al limpiar:\n{result.stderr}")
    print(result.stdout)
    return base_dir / "data" / "cleaned" / "movielens-clean.parquet"

@task
def dvc_add_task(cleaned_path: Path):
    """
    Agrega el parquet limpio a DVC.
    """
    result = subprocess.run(
        ["dvc", "add", str(cleaned_path)],
        cwd=str(cleaned_path.parents[1]),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 and "is already tracked" not in result.stderr:
        raise RuntimeError(f"Error en dvc add:\n{result.stderr}")
    print(result.stdout or "→ Parquet ya trackeado por DVC.")
    return True

@flow(name="ETL-MovieLens")
def etl_movielens_flow():
    """
    Flow principal: limpia MovieLens y versiona con DVC.
    """
    cleaned = clean_movielens_task()
    dvc_add_task(cleaned)

if __name__ == "__main__":
    etl_movielens_flow()
