# ============================================================
# unit_test.py — Pruebas UNITARIAS del proyecto YT_ELT
# ============================================================
# PROPÓSITO:
#   Verificar piezas pequeñas y aisladas. Estas pruebas no llaman
#   a la API de YouTube ni a Postgres: usan los fixtures falsos
#   (mocks) definidos en conftest.py.
#
# ¿CÓMO RECIBE CADA TEST SUS DATOS?
#   pytest mira el NOMBRE del parámetro de la función y busca un
#   fixture con ese mismo nombre en conftest.py. Si el parámetro
#   no coincide con ningún fixture, la variable no existe y da
#   NameError (error corregido en esta sesión: test_channel_handle
#   pedía "api_key" pero usaba "channel_handle").
#
# CÓMO EJECUTAR (dentro del contenedor):
#   pytest -v tests/unit_test.py                          # todas
#   pytest -v tests/unit_test.py -k test_channel_handle   # solo una
# ============================================================


# Comprueba que la Variable API_KEY se lee del entorno mockeado.
def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


# Comprueba que la Variable CHANNEL_HANDLE se lee del entorno mockeado.
# El parámetro debe llamarse igual que el fixture: channel_handle.
def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"


# Comprueba que la Conexión de Postgres se arma con los datos falsos.
# Es lo mismo que haría PostgresHook(postgres_conn_id="postgres_db_yt_elt")
# en dags/datawarehouse/data_utils.py, pero sin conectarse a nada.
def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"  # schema es el nombre de la BD


# Verifica la integridad estructural de todos los DAGs sin ejecutar
# ningún pipeline. DagBag lee dags/ igual que el scheduler de Airflow.
# Si algo falla aquí, el pipeline fallaría a su hora programada.
# Se usa el fixture "dagbag" de conftest.py.
def test_dags_integrity(dagbag):

    # 1. Ningún archivo de dags/ debe tener errores de Python.
    #    import_errors es un dict vacío {} si todo está bien.
    #    Si hay un error, muestra el archivo y el mensaje de error.
    assert dagbag.import_errors == {}, f"Import error found: {dagbag.import_errors}"
    print("============")
    print(dagbag.import_errors)

    # 2. Los 3 DAGs esperados deben existir por nombre exacto.
    #    Si un dag_id cambia de nombre o se borra, el loop lo detecta.
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("============")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."

    # 3. Debe haber exactamente 3 DAGs — ni más ni menos.
    #    Complementa el paso 2: el paso 2 verifica que los esperados
    #    existen; este verifica que no hay DAGs extra o duplicados.
    assert dagbag.size() == 3
    print("============")
    print(dagbag.size())

    # 4. Cada DAG debe tener exactamente el número de tareas esperado.
    #    Si alguien borra o agrega una tarea @task sin querer, falla aquí
    #    con un mensaje que indica cuántas hay vs cuántas se esperaban.
    #
    #    Conteo actualizado tras la re-estructura de DAGs (sesión DAGs Re-Structure):
    #      produce_json: +1 tarea → trigger_update_db (TriggerDagRunOperator)
    #      update_db:    +1 tarea → trigger_data_quality (TriggerDagRunOperator)
    #      data_quality: sin cambios
    expected_task_count = {
        "produce_json": 5,   # get_playlist_id, get_video_ids, extract_video_data, save_to_json, trigger_update_db
        "update_db": 3,      # staging_table, core_table, trigger_data_quality
        "data_quality": 2    # soda_test_staging, soda_test_core
    }
    print("================")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_count[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
        print(dag_id, len(dag.tasks))