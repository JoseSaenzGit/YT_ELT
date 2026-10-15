# =============================================================================
# Dockerfile — YT_ELT Project
# =============================================================================
# Este archivo le dice a Docker cómo construir la imagen del proyecto.
# Una imagen es como una "foto" de un sistema completo y listo para correr.
#
# Para construir la imagen desde la terminal:
#   docker build -t josesaenzpersonal/yt_api_elt:1.0.0 .
#
# El punto (.) al final le indica a Docker que busque este archivo
# en la carpeta actual.
# =============================================================================


# -----------------------------------------------------------------------------
# ARG — Variables de construcción
# -----------------------------------------------------------------------------
# ARG define variables que solo existen mientras Docker construye la imagen.
# Se definen aquí arriba para que sea fácil actualizarlas en el futuro.
# Si sale una versión nueva de Airflow, solo cambia el número aquí
# y el resto del archivo se actualiza automáticamente.
# -----------------------------------------------------------------------------

ARG AIRFLOW_VERSION=2.9.2   # Versión de Apache Airflow (usamos la del tutorial)
ARG PYTHON_VERSION=3.10     # Versión de Python compatible con Airflow 2.9.2


# -----------------------------------------------------------------------------
# FROM — Imagen base
# -----------------------------------------------------------------------------
# FROM le dice a Docker: "parte desde esta imagen ya existente".
# En lugar de instalar Linux + Python + Airflow desde cero, usamos una imagen
# oficial de Apache Airflow que ya tiene todo eso listo.
#
# El tag tiene el formato: [versión-airflow]-python[versión-python]
# Las variables ${} toman los valores de los ARG definidos arriba.
#
# Esta imagen se descarga automáticamente desde Docker Hub:
#   https://hub.docker.com/r/apache/airflow/tags
# -----------------------------------------------------------------------------

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}


# -----------------------------------------------------------------------------
# ENV — Variables de entorno del contenedor
# -----------------------------------------------------------------------------
# ENV define variables que estarán disponibles mientras el contenedor corre.
# A diferencia de ARG, estas sí persisten dentro del contenedor en ejecución.
#
# AIRFLOW_HOME le indica a Airflow en qué carpeta guardar sus archivos:
#   - Configuración
#   - DAGs (los pipelines de datos)
#   - Logs
#
# /opt/airflow es la ruta estándar dentro del contenedor (sistema Linux).
# -----------------------------------------------------------------------------

ENV AIRFLOW_HOME=/opt/airflow


# -----------------------------------------------------------------------------
# COPY — Copiar archivos de tu Mac al contenedor
# -----------------------------------------------------------------------------
# COPY toma un archivo de tu máquina local y lo pone dentro del contenedor.
# Sintaxis: COPY [origen en tu Mac] [destino en el contenedor]
#
# Aquí copiamos requirements.txt a la raíz (/) del contenedor
# para que el siguiente paso RUN pueda encontrarlo.
# -----------------------------------------------------------------------------

COPY requirements.txt /


# -----------------------------------------------------------------------------
# RUN — Ejecutar comandos durante la construcción
# -----------------------------------------------------------------------------
# RUN ejecuta un comando de terminal mientras Docker construye la imagen.
# Este comando instala los paquetes Python necesarios con pip.
#
# Desglose del comando:
#   pip install             → instala paquetes Python
#   --no-cache-dir          → no guarda caché → imagen más liviana
#   "apache-airflow==2.9.2" → instala la versión exacta de Airflow
#                             (== es el operador de versión exacta en pip)
#   -r /requirements.txt    → además instala todo lo listado en ese archivo
#                             (paquetes extras que necesita el proyecto)
# -----------------------------------------------------------------------------

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
