# ============================================================
# main.py — DAGs del pipeline YT_ELT
# ============================================================
# PROPÓSITO:
#   Define los TRES DAGs de Airflow del pipeline ELT de YouTube,
#   encadenados con TriggerDagRunOperator para garantizar que
#   corren en orden y solo si el DAG anterior tuvo éxito.
#
#   1) "produce_json"  → fase EXTRACT  (tiene schedule: 14:00)
#      API de YouTube → archivo JSON en ./data
#      Al terminar dispara → "update_db"
#
#   2) "update_db"     → fase LOAD (+ transformación)  (schedule=None)
#      JSON → schema "staging" → schema "core" en Postgres
#      Al terminar dispara → "data_quality"
#
#   3) "data_quality"  → fase QUALITY  (schedule=None)
#      Soda Core checks contra staging y core
#
# ¿QUÉ ES UN DAG?
#   DAG = Directed Acyclic Graph (Grafo Acíclico Dirigido).
#   Es la unidad de trabajo de Airflow — define:
#     - Qué tareas se ejecutan.
#     - En qué orden se ejecutan (dependencias).
#     - Cuándo se ejecutan (schedule).
#     - Cómo se comportan si fallan (default_args).
#
# FLUJO COMPLETO (DAG dependencies con TriggerDagRunOperator):
#
#   [produce_json]
#     get_playlist_id()
#         ↓
#     get_video_ids(playlist_id)
#         ↓
#     extract_video_data(videos_ids)
#         ↓
#     save_to_json(extract_data)
#         ↓
#     trigger_update_db  ──────────► [update_db]
#                                      staging_table()
#                                          ↓
#                                      core_table()
#                                          ↓
#                                      trigger_data_quality ──► [data_quality]
#                                                                 soda_validate_staging
#                                                                     ↓
#                                                                 soda_validate_core
#
# SCHEDULES:
#   produce_json  → "0 14 * * *" → todos los días a las 14:00 (CR)
#   update_db     → None  (solo se lanza desde produce_json)
#   data_quality  → None  (solo se lanza desde update_db)
#
# ✅ Con TriggerDagRunOperator, si produce_json falla, update_db
#   NO se dispara. Si update_db falla, data_quality NO se dispara.
#   Esto elimina el riesgo de correr pasos downstream sobre datos
#   incompletos o inexistentes.
# ============================================================

from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ----------------------------------------------------------
# IMPORT DE LAS TAREAS
# ----------------------------------------------------------
# Se importan las 4 funciones decoradas con @task desde
# dags/api/video_stats.py.
# Al estar decoradas con @task, Airflow las trata como
# nodos del DAG — no como funciones Python normales.
# ----------------------------------------------------------
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

# Tareas de la fase LOAD, definidas en dags/datawarehouse/dwh.py:
#   staging_table() → carga el JSON del día al schema "staging"
#   core_table()    → transforma staging y lo carga al schema "core"
# Se importan como "datawarehouse.dwh" (sin "dags.") porque
# Airflow agrega la carpeta dags/ al PYTHONPATH.
from datawarehouse.dwh import staging_table, core_table

# Tarea de calidad de datos (Soda), definida en dags/dataquality/soda.py:
#   yt_elt_data_quality(schema) → BashOperator que corre "soda scan"
#   contra el schema indicado ("staging" o "core").
from dataquality.soda import yt_elt_data_quality

# ----------------------------------------------------------
# TIMEZONE
# ----------------------------------------------------------
# pendulum es la librería de manejo de fechas que usa Airflow.
# Se define la zona horaria local para que el schedule
# se interprete en hora de Costa Rica y no en UTC.
# Como start_date (en default_args) lleva este tzinfo, los
# cron de AMBOS DAGs se evalúan en esta zona horaria.
# ----------------------------------------------------------
local_tz = pendulum.timezone("America/Costa_Rica")

# ----------------------------------------------------------
# DEFAULT ARGS
# ----------------------------------------------------------
# Diccionario con configuración que aplica a TODAS las tareas
# por defecto. Cada @task hereda estos valores.
# Es compartido por los dos DAGs (produce_json y update_db).
#
# owner          → quién es responsable del DAG
# depends_on_past→ si True, una tarea no corre hasta que la
#                  ejecución del día anterior haya sido exitosa
# email_on_failure/retry → si enviar correo al fallar/reintentar
# max_active_runs → cuántos DAG runs pueden correr en paralelo
# dagrun_timeout  → tiempo máximo para completar todo el DAG
# start_date      → fecha a partir de la cual Airflow programa runs
# ----------------------------------------------------------
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jose.rodolfo.saenz@ibm.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 1, 1, tzinfo=local_tz)
}

#variables
staging_schema = "staging"
core_schema = "core"


# ----------------------------------------------------------
# DAG 1: produce_json — fase EXTRACT
# ----------------------------------------------------------
# "with DAG(...) as dag:" es el patrón estándar para definir
# un DAG en Airflow. Todo lo que está dentro del bloque with
# pertenece a este DAG.
#
# dag_id     → nombre único del DAG (visible en la UI)
# description→ texto descriptivo visible en la UI
# schedule   → expresión cron que define cuándo corre
#              "0 14 * * *" = todos los días a las 14:00
#              Formato: minuto hora día mes día_semana
# catchup    → si False, Airflow NO crea runs retroactivos
#              para fechas pasadas desde start_date.
#              Si True, correría el DAG para cada día
#              desde start_date hasta hoy — ¡cuidado con esto!
# ----------------------------------------------------------
with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *',
    catchup=False
) as dag_produce:

    # ----------------------------------------------------------
    # DEFINICIÓN DE TAREAS
    # ----------------------------------------------------------
    # Con el TaskFlow API, llamar a una función @task dentro del
    # bloque "with DAG" la registra como una tarea del DAG y
    # crea un objeto XComArg que representa su output futuro.
    #
    # Airflow NO ejecuta estas funciones aquí — solo las registra.
    # Las ejecuta cuando el scheduler dispara el DAG según el schedule.
    # ----------------------------------------------------------

    # Tarea 1: obtiene el playlistId del canal
    playlist_id = get_playlist_id()

    # Tarea 2: obtiene todos los videoIds de la playlist
    # Recibe el output de playlist_id via XCom automáticamente
    videos_ids = get_video_ids(playlist_id)

    # Tarea 3: extrae métricas de cada video en lotes de 50
    # Recibe el output de videos_ids via XCom automáticamente
    extract_data = extract_video_data(videos_ids)

    # Tarea 4: guarda los datos en ./data/YT_data_YYYY-MM-DD.json
    # Recibe el output de extract_data via XCom automáticamente
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id = "trigger_update_db",
        trigger_dag_id = "update_db"
    )

    # ----------------------------------------------------------
    # DEPENDENCIAS entre tareas
    # ----------------------------------------------------------
    # El operador >> define el orden de ejecución:
    # A >> B significa "B se ejecuta después de que A termine".
    #
    # Con el TaskFlow API, las dependencias ya se infieren
    # automáticamente por los argumentos que se pasan entre
    # funciones — esta línea es explícita pero redundante aquí.
    # Se mantiene como buena práctica de documentación visual.
    # ----------------------------------------------------------
    playlist_id >> videos_ids >> extract_data >> save_to_json_task >> trigger_update_db


# ----------------------------------------------------------
# DAG 2: update_db — fase LOAD (+ transformación)
# ----------------------------------------------------------
# Carga a Postgres (BD "elt_db") el JSON que dejó produce_json:
#   staging_table() → JSON crudo           → schema "staging"
#   core_table()    → staging transformado → schema "core"
#   trigger_data_quality → dispara el DAG "data_quality"
#
# schedule=None → este DAG ya NO tiene horario propio.
#   Solo se ejecuta cuando produce_json lo dispara mediante
#   TriggerDagRunOperator. Si produce_json falla, este DAG
#   nunca se lanza.
#
# Se usa "as dag_update" (y no "as dag") para no pisar la
# variable dag_produce del primer bloque en este módulo.
#
# ¿Por qué un DAG separado y no más tareas en produce_json?
#   Desacopla la extracción de la carga: si Postgres falla se
#   reintenta update_db sin volver a llamar a la API de YouTube
#   (que tiene cuota diaria).
# ----------------------------------------------------------
with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data into both staging and core schemas',
    #schedule='0 15 * * *',
    catchup=False,
    schedule=None,
) as dag_update:

    # Llamar a una función @task dentro del "with DAG" la registra
    # como tarea de este DAG (no se ejecuta aquí).
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality"
    )

    # core necesita que staging ya esté actualizado: core_table
    # lee de staging.yt_api, así que debe correr DESPUÉS.
    # A diferencia de produce_json, aquí las tareas no se pasan
    # datos por XCom (cada una lee de su fuente), por eso esta
    # línea NO es redundante: es la única que define el orden.
    update_staging >> update_core >> trigger_data_quality
    

# ----------------------------------------------------------
# DAG 3: data_quality — pruebas de calidad con Soda
# ----------------------------------------------------------
# Corre los checks de include/soda/checks.yml contra staging y
# luego contra core. Si un check falla, "soda scan" termina con
# error y la tarea queda en "failed".
#
# schedule=None → este DAG ya NO tiene horario propio.
#   Solo se ejecuta cuando update_db lo dispara mediante
#   TriggerDagRunOperator. Si update_db falla, este DAG
#   nunca se lanza.
# Se usa "as dag_quality" para no pisar "dag_produce" ni "dag_update".
# ----------------------------------------------------------
with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to check the data quality on both layers in the db',
    #schedule='0 16 * * *',
    catchup=False,
    schedule=None
) as dag_quality:

    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    soda_validate_staging >> soda_validate_core
    