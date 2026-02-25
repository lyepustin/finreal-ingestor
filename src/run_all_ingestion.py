"""
Runner que ejecuta en orden: BBVA → Caixa → Ruralvia → Update Database.
Se detiene en el primer fallo (exit code distinto de 0).
"""
import logging
import os
import subprocess
import sys
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Scripts a ejecutar en orden (relativos a src/)
SCRIPTS = [
    "run_bbva_scraper.py",
    "run_caixa_scraper.py",
    "run_ruralvia_scraper.py",
    "run_update_database.py",
]


def run_script(script_name: str) -> int:
    """Ejecuta un script de Python y devuelve su exit code."""
    src_dir = Path(__file__).resolve().parent
    project_root = src_dir.parent
    script_path = src_dir / script_name
    if not script_path.exists():
        logger.error("Script no encontrado: %s", script_path)
        return 1
    logger.info("Ejecutando: %s", script_name)
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join([str(src_dir), env.get("PYTHONPATH", "")])
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        env=env,
    )
    return result.returncode


def main() -> int:
    logger.info("Iniciando pipeline de ingestión (BBVA → Caixa → Ruralvia → Update DB)")
    for script in SCRIPTS:
        exit_code = run_script(script)
        if exit_code != 0:
            logger.error("Falló %s con exit code %d. Pipeline detenido.", script, exit_code)
            return exit_code
        logger.info("Completado: %s", script)
    logger.info("Pipeline de ingestión finalizado correctamente.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
