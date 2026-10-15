#!/bin/bash

# =============================================================================
# init-multiple-databases.sh
# =============================================================================
# Este script crea los 3 usuarios y las 3 bases de datos de Postgres que
# necesita el proyecto. Postgres lo ejecuta AUTOMÁTICAMENTE la primera vez
# que arranca el contenedor, porque está montado en:
#   /docker-entrypoint-initdb.d/
#
# Las 3 bases de datos que crea:
#   1. airflow_metadata_db  → Estado interno de Airflow (DAGs, runs, logs)
#   2. celery_results_db    → Resultados de tareas ejecutadas por los workers
#   3. elt_db               → Datos extraídos de la API de YouTube
#
# Las credenciales de cada BD vienen del .env, inyectadas como variables
# de entorno por docker-compose.yaml.
#
# ⚠️ IMPORTANTE: Este script necesita permisos de ejecución para correr.
#    Si Postgres falla al arrancar, verifica con:
#    chmod +x docker/postgres/init-multiple-databases.sh
# =============================================================================

# Termina el script inmediatamente si cualquier comando falla.
# Evita que el script siga corriendo con errores silenciosos.
set -e

# Trata las variables no definidas como errores.
# Si una variable del .env no está definida, el script falla con un mensaje claro.
set -u

# =============================================================================
# FUNCIÓN: create_user_and_database
# =============================================================================
# Crea un usuario y una base de datos en Postgres, y le otorga todos los
# permisos al usuario sobre esa base de datos.
#
# Argumentos:
#   $1 → database: nombre de la base de datos a crear
#   $2 → username: nombre del usuario a crear
#   $3 → password: contraseña del usuario
#
# Usa psql (herramienta de línea de comandos de Postgres) para ejecutar SQL.
# El "heredoc" <<-EOSQL ... EOSQL permite escribir SQL en múltiples líneas.
# =============================================================================
function create_user_and_database() {
    local database=$1
    local username=$2
    local password=$3
    echo "Creating user '$username' and database '$database'"
    # Se conecta como el superusuario de Postgres (POSTGRES_USER del .env)
    # y ejecuta los 3 comandos SQL:
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE USER $username WITH PASSWORD '$password';   -- Crea el usuario con su contraseña
        CREATE DATABASE $database;                         -- Crea la base de datos
        GRANT ALL PRIVILEGES ON DATABASE $database TO $username;  -- Le da acceso total al usuario
EOSQL
    echo "  User '$username' and database '$database' created successfully"
}

# =============================================================================
# EJECUCIÓN: llamar la función 3 veces, una por cada BD
# =============================================================================
# Las variables vienen del .env a través de docker-compose.yaml

# 1. BD interna de Airflow → guarda DAGs, ejecuciones, conexiones, variables, etc.
create_user_and_database $METADATA_DATABASE_NAME $METADATA_DATABASE_USERNAME $METADATA_DATABASE_PASSWORD

# 2. BD de resultados de Celery → guarda el estado de cada tarea ejecutada por los workers
create_user_and_database $CELERY_BACKEND_NAME $CELERY_BACKEND_USERNAME $CELERY_BACKEND_PASSWORD

# 3. BD del proyecto ELT → aquí se cargarán los datos de la API de YouTube
create_user_and_database $ELT_DATABASE_NAME $ELT_DATABASE_USERNAME $ELT_DATABASE_PASSWORD

echo "All databases and users created successfully"
