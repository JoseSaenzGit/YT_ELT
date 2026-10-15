# ============================================================
# dwh.py
# ============================================================
# PROPÓSITO:
#   Define las dos tareas de Airflow (@task) de la fase LOAD del
#   pipeline YT_ELT. Cada tarea sincroniza una capa del data
#   warehouse en Postgres (BD "elt_db"):
#
#   staging_table() → JSON crudo de la API  → schema "staging"
#   core_table()    → staging transformado  → schema "core"
#
# FLUJO COMPLETO DEL PIPELINE:
#   API de YouTube → JSON (./data) → staging → core
#   (main.py / video_stats.py)  (staging_table) (core_table)
#
# LÓGICA DE SINCRONIZACIÓN (igual en ambas tareas):
#   - Video nuevo (ID no existe en la tabla)      → INSERT
#   - Video que ya existe (métricas pueden variar) → UPDATE
#   - Video que ya no está en la fuente           → DELETE
#
# DÓNDE SE USAN:
#   dags/main.py las importa y las encadena en el DAG "update_db":
#       staging_table() >> core_table()
#   (schedule "0 15 * * *"; corre una hora después de "produce_json",
#   que es quien deja el JSON del día en ./data).
#   Todavía NO se ha hecho una corrida real contra Postgres.
#
# NOTA SOBRE LOS IMPORTS:
#   Se importa como "datawarehouse.xxx" (sin "dags."). Esto funciona
#   porque Airflow agrega la carpeta dags/ al PYTHONPATH.
# ============================================================

from datawarehouse.data_utils import get_conn_cursor,close_conn_cursor,create_schema,create_table,get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows,update_rows,delete_rows
from datawarehouse.data_transformation import transforme_data

import logging
from airflow.decorators import task

# Logger del módulo — los mensajes aparecen en los logs de cada tarea en la UI de Airflow
logger = logging.getLogger(__name__)

# Nombre de la tabla — debe coincidir con data_utils.py y data_modification.py
table = "yt_api"


# ----------------------------------------------------------
# TAREA: staging_table
# ----------------------------------------------------------
# Carga el JSON del día en el schema "staging".
#
# Aquí los datos van tal cual los devuelve la API, por eso las
# claves del dict son snake_case (video_id, title, publishedAt...).
# La duración todavía es un string ISO 8601 ("PT1H23M45S").
#
# Pasos:
#   1. Abre la conexión y lee el JSON de ./data
#   2. Crea el schema y la tabla si no existen (idempotente)
#   3. Lee los IDs que ya están en la tabla
#   4. Por cada video del JSON: INSERT si es nuevo, UPDATE si ya existe
#   5. Borra de la tabla los IDs que ya no vienen en el JSON
# ----------------------------------------------------------
@task
def staging_table():
    schema = 'staging'

    # Se inicializan en None para que el bloque finally pueda
    # saber si la conexión llegó a abrirse antes de cerrarla
    conn, cur = None, None

    try:
        conn,cur = get_conn_cursor()

        # Lista de dicts: un dict por video
        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        # IDs actuales en la tabla — sirve para decidir entre INSERT y UPDATE
        table_ids = get_video_ids(cur,schema)

        for row in YT_data:
            # Tabla vacía (primera corrida): todo es INSERT
            if len(table_ids) == 0:
                insert_rows(cur,conn,schema,row)

            else:
                # En staging la clave del ID es 'video_id' (nombre de la API)
                if row['video_id'] in table_ids:
                    update_rows(cur,conn,schema,row)
                else:
                    insert_rows(cur,conn,schema,row)
        
        # Set con los IDs presentes en el JSON de hoy
        ids_in_json ={row['video_id'] for row in YT_data}

        # Diferencia de conjuntos: están en la tabla pero ya no en el JSON
        # → el video fue eliminado del canal, se borra de la BD
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur,conn,schema,ids_to_delete)

        logger.info(f"{schema} table update complete")

    except Exception as e:
        logger.error(f"An error ocurred during the update of {schema} table: {e}")
        # Se relanza para que Airflow marque la tarea como FALLIDA.
        # Sin el raise la tarea aparecería como exitosa aunque haya fallado.
        raise e 

    finally:
        # Se ejecuta siempre (con o sin error) para no dejar conexiones abiertas
        if conn and cur:
            close_conn_cursor(conn,cur)


# ----------------------------------------------------------
# TAREA: core_table
# ----------------------------------------------------------
# Toma los datos de "staging", los transforma y los carga en
# el schema "core" (la capa limpia, lista para análisis).
#
# A diferencia de staging, aquí la fuente es la propia BD
# (SELECT * FROM staging.yt_api), no el JSON. Por eso las claves
# del dict son las columnas de Postgres (PascalCase: Video_ID...).
#
# Transformaciones (ver data_transformation.py):
#   - Duration   → de string ISO 8601 a objeto time
#   - Video_Type → nueva columna: "Shorts" (<= 60 s) o "Normal"
#
# Debe correr DESPUÉS de staging_table.
# ----------------------------------------------------------
@task
def core_table():

    schema = 'core'

    conn,cur = None, None

    try:
        conn,cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        # IDs que ya existen en core
        table_ids = get_video_ids(cur,schema)

        # IDs que hay en staging ahora mismo — se llena dentro del loop
        current_videos_ids = set()

        # Lee toda la tabla de staging. El cursor es RealDictCursor,
        # así que cada fila es un dict {columna: valor}
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:
            current_videos_ids.add(row['Video_ID'])

            # Convierte Duration y agrega Video_Type
            transformed_row = transforme_data(row)

            # INSERT si el video no está en core, UPDATE si ya existe
            if len(table_ids) == 0 or row['Video_ID'] not in table_ids:
                insert_rows(cur,conn,schema,transformed_row)
            else:
                update_rows(cur,conn,schema,transformed_row)

        # Videos que están en core pero ya no en staging → se borran
        ids_to_delete = set(table_ids) - current_videos_ids

        if ids_to_delete:
            delete_rows(cur,conn,schema,ids_to_delete)

        logger.info(f"{schema} table update complete")

    except Exception as e:
        logger.error(f"An error ocurred during the update of {schema} table: {e}")
        # Relanza la excepción para que Airflow marque la tarea como fallida
        raise

    finally:
        if conn and cur:
            close_conn_cursor(conn,cur)
