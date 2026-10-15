# ============================================================
# data_loading.py
# ============================================================
# PROPÓSITO:
#   Lee del disco el archivo JSON que generó la tarea save_to_json
#   (dags/api/video_stats.py) y lo devuelve como lista de dicts.
#   Es el punto de entrada de los datos a la fase LOAD.
#
# FUNCIONES:
#   load_data() → list[dict] con un dict por video
# ============================================================

import json
from datetime import date
import logging

# Logger del módulo — los mensajes aparecen en los logs de Airflow
logger = logging.getLogger(__name__)


# ----------------------------------------------------------
# FUNCIÓN: load_data
# ----------------------------------------------------------
# Abre ./data/YT_data_YYYY-MM-DD.json con la fecha de HOY.
#
# La ruta es relativa y coincide con la de save_to_json(). Dentro
# del contenedor resuelve a /opt/airflow/data, carpeta montada
# desde ./data de tu Mac (ver volumes en docker-compose.yaml).
#
# Importante: como el nombre lleva date.today(), si el JSON se
# generó otro día esta función no lo encuentra y lanza
# FileNotFoundError.
#
# encoding='utf-8' es necesario porque los títulos de los videos
# pueden tener tildes, emojis y otros caracteres especiales.
#
# Errores:
#   FileNotFoundError → no existe el archivo del día
#   JSONDecodeError   → el archivo existe pero no es JSON válido
#   En ambos casos se registra el error y se relanza (raise)
#   para que Airflow marque la tarea como fallida.
# ----------------------------------------------------------
def load_data():
    file_path = f"./data/YT_data_{date.today()}.json"

    try:
        logger.info(f"Processing file: YT_data_{date.today()}")

        with open(file_path,'r',encoding='utf-8') as raw_data:
            data = json.load(raw_data)

        return data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid Json in file: {file_path}")
        raise
