# ============================================================
# data_utils.py
# ============================================================
# PROPÓSITO:
#   Funciones de utilidad para trabajar con Postgres:
#   conexión, creación de schemas/tablas (DDL) y consultas
#   de apoyo. Las usa dwh.py.
#
# FUNCIONES:
#   get_conn_cursor()        → abre conexión + cursor
#   close_conn_cursor()      → cierra cursor + conexión
#   create_schema(schema)    → CREATE SCHEMA IF NOT EXISTS
#   create_table(schema)     → CREATE TABLE IF NOT EXISTS
#   get_video_ids(cur, schema) → lista de Video_ID de la tabla
#
# SCHEMAS DEL DATA WAREHOUSE:
#   staging → datos crudos de la API (duración como texto ISO 8601)
#   core    → datos transformados (agrega la columna Video_Type)
# ============================================================

# PostgresHook: clase de Airflow que lee la conexión registrada en
#   Airflow (aquí viene de la variable AIRFLOW_CONN_POSTGRES_DB_YT_ELT
#   definida en docker-compose.yaml) y abre la conexión con psycopg2.
from airflow.providers.postgres.hooks.postgres import PostgresHook

# RealDictCursor: hace que cada fila se devuelva como dict
#   {"Video_ID": "abc"} en vez de tupla ("abc",). Así se accede
#   por nombre de columna: row["Video_ID"].
#   (psycopg2 viene incluido en la imagen de Airflow.)
from psycopg2.extras import RealDictCursor

# Nombre de la tabla — igual en staging y en core, solo cambia el schema
table = "yt_api"


# ----------------------------------------------------------
# FUNCIÓN: get_conn_cursor
# ----------------------------------------------------------
# Abre una conexión a la BD "elt_db" y crea un cursor.
#
# postgres_conn_id → ID de la conexión de Airflow. Los IDs de
#   conexión no distinguen mayúsculas de minúsculas, por eso
#   "postgres_db_yt_elt" encuentra AIRFLOW_CONN_POSTGRES_DB_YT_ELT.
#
# Retorna:
#   (conn, cur) → la conexión (para commit) y el cursor (para execute)
# ----------------------------------------------------------
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt",database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    return conn,cur


# ----------------------------------------------------------
# FUNCIÓN: close_conn_cursor
# ----------------------------------------------------------
# Cierra primero el cursor y luego la conexión.
# Siempre hay que llamarla al terminar para no dejar
# conexiones abiertas en Postgres.
# ----------------------------------------------------------
def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()


# ----------------------------------------------------------
# FUNCIÓN: create_schema
# ----------------------------------------------------------
# Crea el schema (staging o core) si no existe.
# IF NOT EXISTS la hace idempotente: se puede ejecutar en
# cada corrida del DAG sin que falle.
#
# Nota: abre y cierra su propia conexión, independiente de la
# que usa la tarea que la llama.
# ----------------------------------------------------------
def create_schema(schema):
    conn , cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)

    # Sin commit el cambio se pierde al cerrar la conexión
    conn.commit()

    close_conn_cursor(conn,cur)


# ----------------------------------------------------------
# FUNCIÓN: create_table
# ----------------------------------------------------------
# Crea la tabla yt_api dentro del schema indicado si no existe.
#
# Diferencia entre schemas:
#   staging → sin Video_Type (los datos vienen tal cual de la API)
#   core    → con Video_Type ("Shorts" / "Normal"), calculado en
#             data_transformation.py
#
# Video_ID es PRIMARY KEY: un video no puede estar dos veces.
# Los nombres de columna van entre comillas dobles para que
# Postgres respete las mayúsculas (Video_ID y no video_id).
#
# Nota: "Duration" es VARCHAR(20) porque se guarda el valor tal
# como se inserta. Si core guarda un objeto time, psycopg2 lo
# convierte a texto "HH:MM:SS".
#
# Nota de seguridad: {schema} se inserta con f-string. Es seguro
# aquí porque solo se llama con valores fijos del código
# ('staging' o 'core'), nunca con datos externos.
# ----------------------------------------------------------
def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT
                )
            """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Type" VARCHAR(10) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT
                )
            """
    cur.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn,cur)


# ----------------------------------------------------------
# FUNCIÓN: get_video_ids
# ----------------------------------------------------------
# Devuelve la lista de Video_ID que ya existen en la tabla.
# dwh.py la usa para decidir qué videos son nuevos (INSERT),
# cuáles ya existen (UPDATE) y cuáles sobran (DELETE).
#
# Recibe el cursor ya abierto (no abre conexión propia).
# Gracias a RealDictCursor cada fila es un dict, por eso
# se accede con row["Video_ID"].
# ----------------------------------------------------------
def get_video_ids(cur,schema):

    cur.execute(f""" SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    videos_ids = [row["Video_ID"] for row in ids]

    return videos_ids
