# ============================================================
# video_stats.py
# ============================================================
# PROPÓSITO:
#   Conectarse a la YouTube Data API v3 para:
#     1. Obtener el ID de la playlist de uploads de un canal.
#     2. Obtener todos los IDs de videos de esa playlist
#        usando paginación automática.
#     3. Extraer métricas detalladas de cada video.
#     4. Guardar los datos extraídos en un archivo JSON local.
#
# CONCEPTOS CLAVE CUBIERTOS EN ESTA LECCIÓN:
#   1. Variables de entorno con .env (seguridad de credenciales)
#   2. Librería python-dotenv para cargar el .env
#   3. Llamadas HTTP con la librería requests
#   4. Manejo de errores con try/except y raise_for_status()
#   5. Parseo de respuestas JSON
#   6. Patrón if __name__ == "__main__" (módulo vs ejecución directa)
#   7. Paginación con nextPageToken en la YouTube Data API v3
#   8. Procesamiento en lotes (batching) para respetar límites de la API
#   9. Escritura de archivos JSON con la librería json
#
# FLUJO DEL SCRIPT:
#   get_playlist_id()
#       └── Endpoint: /youtube/v3/channels
#       └── Retorna: playlistId (str)
#            ↓
#   get_video_ids(playlistId)
#       └── Endpoint: /youtube/v3/playlistItems
#       └── Pagina con nextPageToken hasta agotar resultados
#       └── Retorna: lista de videoIds (list[str])
#            ↓
#   extract_video_data(video_ids)
#       └── Endpoint: /youtube/v3/videos
#       └── Procesa los IDs en lotes de 50 (límite de la API)
#       └── Retorna: lista de dicts con métricas por video (list[dict])
#            ↓
#   save_to_json(extracted_data)
#       └── Crea carpeta ./data/ si no existe
#       └── Guarda el archivo: ./data/YT_data_YYYY-MM-DD.json
# ============================================================

from datetime import date
import statistics
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

maxResults = 50



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


def get_video_ids(playlistId):
    """
    Obtiene todos los IDs de videos de una playlist de YouTube,
    manejando automáticamente la paginación con nextPageToken.

    Endpoint usado:
        GET /youtube/v3/playlistItems
        Parámetros: part=contentDetails, maxResults=50,
                    playlistId=<id>, key=<api_key>,
                    pageToken=<token> (solo en páginas siguientes)

    La API de YouTube devuelve máximo 50 resultados por llamada.
    Cuando hay más videos, la respuesta incluye un campo
    'nextPageToken'. Este loop continúa hasta que ese campo
    ya no exista, lo que indica que se llegó a la última página.

    Estructura del JSON de respuesta:
        data
         └── items[]
              └── contentDetails
                   └── videoId  ← ID único del video

    Args:
        playlistId (str): El ID de la playlist de uploads del canal.

    Retorna:
        list[str]: Lista con todos los videoIds de la playlist.

    Lanza:
        requests.exceptions.HTTPError si la API responde con error.
    """
    # Acumula todos los IDs de video encontrados en todas las páginas
    videos_ids = []

    # Token de la siguiente página; None en la primera iteración
    pageToken = None

    # URL base sin pageToken (se agrega dinámicamente si existe)
    base_url = (
        f"https://youtube.googleapis.com/youtube/v3/playlistItems"
        f"?part=contentDetails"
        f"&maxResults={maxResults}"
        f"&playlistId={playlistId}"
        f"&key={API_KEY}"
    )

    try:
        # Loop de paginación: continúa mientras haya nextPageToken
        while True:
            url = base_url

            # Si existe un token de página, se agrega a la URL
            # para que la API devuelva el siguiente bloque de resultados
            if pageToken:
                url += f"&pageToken={pageToken}"

            # Petición HTTP GET a la API de YouTube
            response = requests.get(url)

            # Lanza excepción si la respuesta es un error HTTP (4xx/5xx)
            response.raise_for_status()

            # Convierte el JSON de respuesta a diccionario Python
            data = response.json()

            # Extrae el videoId de cada item de la página actual
            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                videos_ids.append(video_id)

            # Obtiene el token para la siguiente página (None si es la última)
            pageToken = data.get('nextPageToken')

            # Si no hay más páginas, termina el loop
            if not pageToken:
                break

        return videos_ids

    except requests.exceptions.RequestException as e:
        # Re-lanza la excepción para manejo en el nivel superior
        raise e


def extract_video_data(video_ids):
    """
    Obtiene métricas detalladas de una lista de videoIds usando la
    YouTube Data API v3, procesando los IDs en lotes de 50.

    ¿Por qué lotes (batching)?
        La API acepta máximo 50 IDs por llamada en el parámetro &id=.
        Si el canal tiene 300 videos, se hacen 6 llamadas de 50 videos
        cada una en lugar de 300 llamadas individuales. Esto es más
        eficiente y respeta los límites (quotas) de la API.

    Endpoint usado:
        GET /youtube/v3/videos
        Parámetros: part=snippet,contentDetails,statistics,
                    id=<id1,id2,...,id50>, key=<api_key>

    Estructura del JSON de respuesta por video:
        item
         ├── id                        ← videoId
         ├── snippet
         │    ├── title                ← título del video
         │    └── publishedAt          ← fecha de publicación (ISO 8601)
         ├── contentDetails
         │    └── duration             ← duración en formato ISO 8601 (PT4M13S)
         └── statistics
              ├── viewCount            ← número de vistas
              ├── likeCount            ← número de likes
              └── commentCount         ← número de comentarios

    Args:
        video_ids (list[str]): Lista de videoIds a consultar.

    Retorna:
        list[dict]: Lista de diccionarios, uno por video, con sus métricas.

    Lanza:
        requests.exceptions.HTTPError si la API responde con error.
    """
    # Lista que acumula un diccionario de métricas por cada video
    extracted_data = []

    # Función interna (generador) que divide una lista en sub-listas de batch_size
    # Ejemplo: [1,2,3,4,5] con batch_size=2 → [1,2], [3,4], [5]
    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id: video_id + batch_size]

    try:
        # Itera sobre cada lote de hasta 50 videoIds
        for batch in batch_list(video_ids, maxResults):

            # Une los IDs del lote en un string separado por comas
            # Ejemplo: "abc123,def456,ghi789"
            video_ids_str = ",".join(batch)

            # Construye la URL pidiendo 3 partes de datos en una sola llamada:
            #   snippet        → título, fecha de publicación, descripción, etc.
            #   contentDetails → duración, definición, etc.
            #   statistics     → vistas, likes, comentarios
            url = (
                f"https://youtube.googleapis.com/youtube/v3/videos"
                f"?part=snippet,contentDetails,statistics"
                f"&id={video_ids_str}"
                f"&key={API_KEY}"
            )

            # Petición HTTP GET a la API de YouTube
            response = requests.get(url)

            # Lanza excepción si la respuesta es un error HTTP (4xx/5xx)
            response.raise_for_status()

            # Convierte el JSON de respuesta a diccionario Python
            data = response.json()

            # Extrae y estructura los campos de cada video del lote
            for item in data.get('items', []):
                video_id     = item['id']
                snippet      = item['snippet']
                contentDetails = item['contentDetails']
                statistics   = item['statistics']

                # Construye un diccionario plano con los campos que nos interesan.
                # statistics.get(..., None) evita KeyError si el canal ocultó ese dato.
                video_data = {
                    "video_id"     : video_id,
                    "title"        : snippet['title'],
                    "publishedAt"  : snippet['publishedAt'],
                    "duration"     : contentDetails['duration'],
                    "viewCount"    : statistics.get('viewCount', None),
                    "likeCount"    : statistics.get('likeCount', None),
                    "commentCount" : statistics.get('commentCount', None)
                }

                # Agrega el dict del video a la lista de resultados
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        # Re-lanza la excepción para manejo en el nivel superior
        raise e


def save_to_json(extracted_data):
    """
    Guarda la lista de métricas de videos en un archivo JSON local.

    El archivo se nombra con la fecha de hoy para facilitar el seguimiento
    histórico de los datos extraídos.
    Ejemplo de nombre: YT_data_2025-10-05.json

    La carpeta ./data/ se crea automáticamente si no existe, evitando
    un FileNotFoundError al ejecutar el script por primera vez.

    Args:
        extracted_data (list[dict]): Lista de dicts con métricas por video.

    Genera el archivo:
        ./data/YT_data_YYYY-MM-DD.json
    """
    # Crea la carpeta ./data/ si no existe (exist_ok=True evita error si ya existe)
    os.makedirs("./data", exist_ok=True)

    # Nombre del archivo con la fecha de hoy como sufijo
    file_path = f"./data/YT_data_{date.today()}.json"

    # Abre (o crea) el archivo en modo escritura con encoding UTF-8
    # indent=4 → formato legible con sangría de 4 espacios
    # ensure_ascii=False → permite caracteres especiales (tildes, emojis, etc.)
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

    print(f"Datos guardados en: {file_path}")


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
    playlistId = get_playlist_id()
    videos_ids = get_video_ids(playlistId)
    video_data = extract_video_data(videos_ids)
    save_to_json(video_data)