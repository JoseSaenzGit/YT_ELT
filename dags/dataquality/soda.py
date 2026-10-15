# ============================================================
# soda.py — Tarea de Airflow que ejecuta los checks de Soda
# ============================================================
# yt_elt_data_quality(schema) crea un BashOperator que corre:
#   soda scan -d pg_datasource -c <configuration.yml> -v SCHEMA=<schema> <checks.yml>
# Si algún check falla, "soda scan" devuelve un código de salida
# distinto de 0 y Airflow marca la tarea como "failed".
#
# Las credenciales de Postgres las lee configuration.yml desde las
# variables de entorno del contenedor (las pasa docker-compose.yaml).
# ============================================================

import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

# Ruta DENTRO del contenedor (./include del Mac está montado ahí).
SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"


def yt_elt_data_quality(schema):
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema}",
            bash_command=(
                f"soda scan -d {DATASOURCE} "
                f"-c {SODA_PATH}/configuration.yml "
                f"-v SCHEMA={schema} "
                f"{SODA_PATH}/checks.yml"
            ),
        )
        return task
    except Exception:
        logger.error("Error running data quality checks for schema: %s", schema)
        raise
