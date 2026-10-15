# ============================================================
# integration_test.py — Pruebas de INTEGRACIÓN del proyecto YT_ELT
# ============================================================
# PROPÓSITO:
#   Verificar que las piezas del sistema funcionan juntas usando
#   servicios y credenciales REALES (no mocks). A diferencia de las
#   pruebas unitarias, estas dependen de que el stack de Docker esté
#   corriendo (Postgres, API de YouTube accesible desde el contenedor).
#
# DIFERENCIA CON UNIT TESTS:
#   - Unitarias  → datos inventados (mocks), no necesitan Docker
#   - Integración → datos reales, requieren Docker corriendo
#
# ¿DE DÓNDE VIENEN LAS CREDENCIALES?
#   Las variables de entorno (API_KEY, ELT_DATABASE_NAME, etc.) vienen
#   del archivo .env → docker-compose.yaml → contenedor airflow-worker.
#   Los fixtures en conftest.py las leen con os.getenv() y preparan
#   los recursos (conexiones, variables) antes de que corra cada test.
#
# CÓMO EJECUTAR (dentro del contenedor airflow-worker):
#   pytest -v -s tests/integration_test.py                            # todos
#   pytest -v -s tests/integration_test.py -k test_youtube_api_response
#   pytest -v -s tests/integration_test.py -k test_real_postgres_connection
# ============================================================

# NOTA sobre imports:
#   - psycopg2    → driver de Python para conectarse a Postgres
#   - requests    → librería HTTP para llamar a la API de YouTube
#   - pytest      → framework de pruebas (pytest.fail detiene el test con mensaje)
import psycopg2
import requests
import pytest


# ------------------------------------------------------------
# Test: test_youtube_api_response
# ------------------------------------------------------------
# Verifica que la API de YouTube responde correctamente con las
# credenciales reales del proyecto (API_KEY y CHANNEL_HANDLE).
#
# Recibe el fixture "airflow_variable" de conftest.py, que es una
# función auxiliar que lee variables del entorno del contenedor
# con el prefijo AIRFLOW_VAR_ (ej: AIRFLOW_VAR_API_KEY).
#
# ¿Qué valida?
#   - Que la API_KEY no haya expirado
#   - Que el CHANNEL_HANDLE sea válido
#   - Que haya conectividad desde el contenedor hacia YouTube
# ------------------------------------------------------------
def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("api_key")
    channel_handle = airflow_variable("channel_handle")

    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200

    except requests.RequestException as e:
        pytest.fail(f"Request to Youtube API failed: {e}")


# ------------------------------------------------------------
# Test: test_real_postgres_connection
# ------------------------------------------------------------
# Verifica que la conexión REAL a Postgres funciona y que el motor
# puede ejecutar una query simple.
#
# Recibe el fixture "real_postgres_connection" de conftest.py, que
# abre la conexión con las credenciales reales del contenedor y la
# cierra automáticamente al terminar el test (patrón yield/teardown).
#
# ¿Qué valida?
#   - Que las credenciales de Postgres son correctas
#   - Que el contenedor postgres está accesible desde airflow-worker
#   - Que el motor puede recibir y ejecutar una query
#
# ¿Por qué SELECT 1?
#   Es la query más simple posible. No importa qué datos haya en la BD,
#   solo queremos confirmar que la conexión está viva y Postgres responde.
#   Es equivalente a hacer "ping" pero para una base de datos.
# ------------------------------------------------------------
def test_real_postgres_connection(real_postgres_connection):
    cursor = None

    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")   # query mínima para verificar conectividad
        result = cursor.fetchone()

        assert result[0] == 1

    except psycopg2.Error as e:
        pytest.fail(f"Database query fail: {e}")

    finally:
        # Cerramos el cursor aquí (la conexión la cierra el fixture en conftest.py)
        if cursor is not None:
            cursor.close()
