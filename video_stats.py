# ============================================================
# video_stats.py
# ============================================================
# PROPÓSITO:
#   Conectarse a la YouTube Data API v3 y obtener el ID de la
#   playlist de uploads de un canal de YouTube.
#
# CONCEPTOS CLAVE CUBIERTOS EN ESTA LECCIÓN:
#   1. Variables de entorno con .env (seguridad de credenciales)
#   2. Librería python-dotenv para cargar el .env
#   3. Llamadas HTTP con la librería requests
#   4. Manejo de errores con try/except y raise_for_status()
#   5. Parseo de respuestas JSON
#   6. Patrón if __name__ == "__main__" (módulo vs ejecución directa)
# ============================================================

from datetime import date
import requests
import json

import os
from dotenv import load_dotenv  # pip install python-dotenv

# ----------------------------------------------------------
# CARGA DEL ARCHIVO .env
# ----------------------------------------------------------
# load_dotenv() lee el archivo .env que está en el mismo
# directorio y carga sus variables al entorno del proceso.
# DEBE llamarse ANTES de cualquier os.getenv() para que
# las variables estén disponibles.
#
# ¿Por qué usamos .env?
#   - Evita hardcodear credenciales en el código fuente.
#   - El archivo .env está en .gitignore → nunca se sube a git.
#   - Cada desarrollador tiene su propio .env local.
# ----------------------------------------------------------
load_dotenv(dotenv_path="./.env")

# ----------------------------------------------------------
# VARIABLES DE CONFIGURACIÓN
# ----------------------------------------------------------
# os.getenv("API_KEY") lee la variable API_KEY del entorno.
# Si .env contiene:  API_KEY=tu_clave_aqui
# entonces API_KEY valdrá esa cadena en tiempo de ejecución.
# Si load_dotenv() no fue llamado antes, retorna None.
API_KEY = os.getenv("API_KEY")

# Handle del canal de YouTube a consultar.
# Se puede cambiar por cualquier canal (ej: "@Fireship", "@TechWithTim")
CHANNEL_HANDLE = "MrBeast"


def get_playlist_id():
    """
    Llama a la YouTube Data API v3 para obtener el ID de la
    playlist de uploads del canal definido en CHANNEL_HANDLE.

    Endpoint usado:
        GET /youtube/v3/channels
        Parámetros: part=contentDetails, forHandle=<handle>, key=<api_key>

    Retorna:
        str: El playlistId de uploads del canal.

    Lanza:
        requests.exceptions.HTTPError si la API responde con error (ej: 400, 403).
    """
    try:
        # Construye la URL con los parámetros de la API.
        # f-string permite insertar variables directamente en la cadena.
        url = (
            f"https://youtube.googleapis.com/youtube/v3/channels"
            f"?part=contentDetails"
            f"&forHandle={CHANNEL_HANDLE}"
            f"&key={API_KEY}"
        )

        # Realiza la petición HTTP GET a la API de YouTube
        response = requests.get(url)

        # raise_for_status() lanza una excepción automáticamente si
        # el servidor respondió con un código de error HTTP (4xx o 5xx).
        # Sin esta línea, requests no lanza error aunque la API falle.
        response.raise_for_status()

        # Convierte la respuesta JSON a un diccionario Python
        data = response.json()

        # DEBUG: descomentar para ver la respuesta completa de la API con formato
        # print(json.dumps(data, indent=4))

        # Navega la estructura del JSON de respuesta:
        # data
        #  └── items[0]
        #       └── contentDetails
        #            └── relatedPlaylists
        #                 └── uploads  ← este es el playlistId que necesitamos
        channel_items = data['items'][0]
        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']

        print(channel_playlistId)

        return channel_playlistId

    except requests.exceptions.RequestException as e:
        # Re-lanza la excepción para que el código que llame a esta
        # función pueda manejarla según su propio contexto.
        raise e


# ----------------------------------------------------------
# PATRÓN if __name__ == "__main__"
# ----------------------------------------------------------
# Python asigna __name__ = "__main__" solo cuando este archivo
# se ejecuta DIRECTAMENTE (ej: python video_stats.py).
#
# Si este archivo se IMPORTA desde otro script
# (ej: import video_stats), __name__ valdrá "video_stats"
# y el bloque de abajo NO se ejecutará.
#
# Esto permite que el archivo funcione tanto como script
# independiente como módulo reutilizable.
# ----------------------------------------------------------
if __name__ == "__main__":
    get_playlist_id()
