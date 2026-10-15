# ============================================================
# conftest.py — Fixtures compartidos por todas las pruebas
# ============================================================
# PROPÓSITO:
#   pytest carga este archivo AUTOMÁTICAMENTE antes de correr
#   los tests de esta carpeta. Aquí se definen los "fixtures":
#   funciones que preparan recursos (datos, conexiones) antes
#   de que corra cada test y los limpian al terminar.
#
# ¿QUÉ ES UN FIXTURE?
#   Es una función marcada con @pytest.fixture. El test la pide
#   poniendo su NOMBRE EXACTO como parámetro. pytest la inyecta
#   automáticamente — no hace falta importarla ni llamarla.
#
# TIPOS DE FIXTURES EN ESTE ARCHIVO:
#   1. Fixtures con MOCK (unitarias): usan datos inventados para
#      no depender de servicios externos. Usan mock.patch.dict para
#      simular variables de entorno solo durante el test.
#   2. Fixtures REALES (integración): leen las variables del entorno
#      del contenedor con os.getenv() y abren conexiones reales.
#
# ¿PARA QUÉ SIRVE "yield" EN UN FIXTURE?
#   Lo que está ANTES del yield es el setup (preparación).
#   Lo que está DESPUÉS del yield es el teardown (limpieza).
#   Si usáramos return, no habría forma de ejecutar la limpieza
#   después del test. yield pausa el fixture, corre el test, y
#   al terminar ejecuta el código que viene después.
#
# ¿CÓMO FUNCIONA EL MOCK?
#   Airflow lee las Variables desde el entorno si el nombre lleva
#   el prefijo AIRFLOW_VAR_, y las Conexiones si lleva AIRFLOW_CONN_.
#   mock.patch.dict("os.environ", ...) agrega esas variables SOLO
#   mientras dura el bloque "with"; al salir, restaura el entorno.
#
# CÓMO EJECUTAR (dentro del contenedor, no en el .venv local):
#   docker exec -it airflow-worker bash
#   pytest -v tests/unit_test.py        # pruebas unitarias
#   pytest -v tests/integration_test.py # pruebas de integración
# ============================================================

import os
import pytest
import psycopg2
from unittest import mock
from airflow.models import Variable, Connection, DagBag


# ------------------------------------------------------------
# Fixture: api_key
# ------------------------------------------------------------
# Simula la Variable de Airflow API_KEY con un valor falso.
# Devuelve lo que Variable.get("API_KEY") lee del entorno.
# Se usa en: test_api_key
# ------------------------------------------------------------
@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")


# ------------------------------------------------------------
# Fixture: channel_handle
# ------------------------------------------------------------
# Simula la Variable de Airflow CHANNEL_HANDLE (el canal de
# YouTube). En la realidad vale "MrBeast"; en la prueba "MRCHEESE".
# Se usa en: test_channel_handle
# ------------------------------------------------------------
@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MRCHEESE"):
        yield Variable.get("CHANNEL_HANDLE")


# ------------------------------------------------------------
# Fixture: mock_postgres_conn_vars
# ------------------------------------------------------------
# Simula la Conexión de Airflow "postgres_db_yt_elt" con datos
# falsos, sin tocar la base de datos real.
#
# Pasos:
#   1) Connection(...) arma el objeto con host, usuario, etc.
#   2) get_uri() lo convierte en una URI de conexión (texto).
#   3) Esa URI se guarda en AIRFLOW_CONN_POSTGRES_DB_YT_ELT, que
#      es el nombre que Airflow busca para el conn_id
#      "postgres_db_yt_elt" (no distingue mayúsculas).
#   4) get_connection_from_secrets la lee de vuelta, igual que lo
#      haría PostgresHook en el código real.
#
# OJO: en Airflow, "schema" de una Connection es el NOMBRE DE LA
#      BASE DE DATOS (elt_db), no un schema de Postgres como
#      "staging" o "core".
# Se usa en: test_postgres_conn
# ------------------------------------------------------------
@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name",  # schema es el nombre de la BD
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")

# ------------------------------------------------------------
# Fixture: dagbag
# ------------------------------------------------------------
# DagBag es el objeto de Airflow que carga y parsea todos los
# archivos .py de la carpeta dags/, igual que lo hace el scheduler
# al arrancar. Si un DAG tiene un error de Python, lo registra en
# dagbag.import_errors en lugar de lanzar una excepción.
#
# include_examples=False → ignora los DAGs de ejemplo de Airflow.
# DagBag se importa dentro del fixture (no al inicio del archivo)
# para evitar que se cargue antes de que pytest configure el entorno.
# Se usa en: test_dags_integrity
# ------------------------------------------------------------
@pytest.fixture
def dagbag():
    from airflow.models import DagBag
    return DagBag(dag_folder="dags/", include_examples=False)

# ------------------------------------------------------------
# Fixture: airflow_variable
# ------------------------------------------------------------
# Fixture de integración — lee Variables REALES de Airflow desde
# el entorno del contenedor (no usa mocks).
#
# Retorna una función auxiliar parametrizada: el test la llama
# pasando el nombre de la variable que quiere leer.
# Ejemplo: airflow_variable("api_key") lee AIRFLOW_VAR_API_KEY
#
# ¿Por qué el prefijo AIRFLOW_VAR_?
#   Airflow almacena sus Variables en el entorno con ese prefijo
#   para distinguirlas de otras variables del sistema.
#   docker-compose.yaml las define así:
#     AIRFLOW_VAR_API_KEY: ${API_KEY}   ← viene del .env
#
# Se usa en: test_youtube_api_response (integration_test.py)
# ------------------------------------------------------------
@pytest.fixture
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)   # lee del entorno real del contenedor

    return get_airflow_variable


# ------------------------------------------------------------
# Fixture: real_postgres_connection
# ------------------------------------------------------------
# Fixture de integración — abre una conexión REAL a Postgres usando
# las credenciales del entorno del contenedor.
#
# ¿De dónde vienen las variables?
#   .env → docker-compose.yaml (environment:) → contenedor airflow-worker
#   → os.getenv() las lee aquí.
#
# Patrón yield (setup / teardown):
#   - SETUP:    lee variables, abre conexión con psycopg2.connect()
#   - yield:    entrega la conexión al test y pausa el fixture
#   - TEARDOWN: cuando el test termina, conn.close() cierra la conexión
#
# ¿Por qué yield y no return?
#   Con return no habría forma de ejecutar conn.close() después del test.
#   Con yield el fixture "espera" a que el test termine para limpiar.
#
# Se usa en: test_real_postgres_connection (integration_test.py)
# ------------------------------------------------------------
@pytest.fixture
def real_postgres_connection():
    # SETUP: lee credenciales reales del entorno del contenedor
    dbname   = os.getenv("ELT_DATABASE_NAME")
    user     = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host     = os.getenv("POSTGRES_CONN_HOST")
    port     = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        yield conn   # entrega la conexión al test; pausa aquí hasta que el test termine

    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to the database: {e}")

    finally:
        # TEARDOWN: se ejecuta siempre, haya pasado o fallado el test
        if conn:
            conn.close()