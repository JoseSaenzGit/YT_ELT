# ============================================================
# data_modification.py
# ============================================================
# PROPÓSITO:
#   Funciones DML (Data Manipulation Language) para el pipeline YT_ELT.
#   Maneja las operaciones de escritura en la BD Postgres:
#   insertar filas nuevas, actualizar filas existentes y
#   eliminar filas que ya no existen en el canal de YouTube.
#
# FUNCIONES:
#   insert_rows(cur, conn, schema, row)         → INSERT de una fila
#   update_rows(cur, conn, schema, row)         → UPDATE de una fila
#   delete_rows(cur, conn, schema, ids_to_delete) → DELETE por IDs
#
# ¿POR QUÉ SEPARAR insert/update/delete?
#   El pipeline necesita hacer un "upsert inteligente":
#   - Videos nuevos   → INSERT
#   - Videos que ya existen pero cambiaron (vistas, likes) → UPDATE
#   - Videos borrados del canal → DELETE
#   Separar las operaciones permite manejar cada caso distinto.
#
# NOTA SOBRE LOS SCHEMAS:
#   staging → nombres de campo de la API (snake_case):
#             video_id, title, publishedAt, duration, etc.
#   core    → nombres de campo estandarizados (PascalCase):
#             Video_ID, Video_Title, Upload_Date, etc.
#   Cada función recibe el schema como argumento y ajusta
#   los nombres de campo correspondientes.
# ============================================================

import json
import logging

# ----------------------------------------------------------
# IMPORTS
# ----------------------------------------------------------
# logging: módulo estándar de Python para registrar eventos.
#   logger.info()  → evento informativo normal
#   logger.error() → evento de error
#   __name__ → el logger toma el nombre del módulo actual,
#   lo que facilita identificar de dónde viene el log.
# ----------------------------------------------------------

# Logger para este módulo — los mensajes aparecen en los logs de Airflow
logger = logging.getLogger(__name__)

# Nombre de la tabla — debe coincidir con el creado en data_utils.py
table = "yt_api"


# ----------------------------------------------------------
# FUNCIÓN: insert_rows
# ----------------------------------------------------------
# Inserta una fila (un video) en la tabla del schema indicado.
#
# Usa queries parametrizados con %(clave)s — esto es fundamental
# para seguridad: evita SQL Injection. psycopg2 escapa los valores
# automáticamente antes de enviarlos a Postgres.
#
# ¿Cómo funciona %(clave)s?
#   El segundo argumento de cur.execute() es el dict "row".
#   psycopg2 reemplaza %(video_id)s con row['video_id'], etc.
#   Ejemplo: si row = {"video_id": "abc123", "title": "My Video", ...}
#   entonces %(video_id)s → 'abc123'
#
# Args:
#   cur    → cursor de psycopg2
#   conn   → conexión de psycopg2 (para commit)
#   schema → "staging" o "core"
#   row    → dict con los datos del video a insertar
# ----------------------------------------------------------
def insert_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            # En staging los keys del dict vienen de la API de YouTube
            videos_id = 'video_id'

            cur.execute(
                f"""
                INSERT INTO {schema}.{table}
                ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """, row
            )
        else:
            # En core los keys del dict vienen transformados (PascalCase)
            videos_id = 'Video_ID'

            cur.execute(
                f"""
                INSERT INTO {schema}.{table}
                ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s);
                """, row
            )

        # Confirma la transacción — sin esto el INSERT no se persiste
        conn.commit()

        logger.info(f"Inserted row with Video_ID: {row[videos_id]}")

    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[videos_id]} - {e}")
        raise e


# ----------------------------------------------------------
# FUNCIÓN: update_rows
# ----------------------------------------------------------
# Actualiza los campos que pueden cambiar con el tiempo:
# título (puede editarse), vistas, likes y comentarios.
#
# NO actualiza Video_ID ni Upload_Date porque son inmutables —
# un video no cambia su ID ni su fecha de publicación.
#
# La cláusula WHERE usa Video_ID Y Upload_Date para identificar
# exactamente qué fila actualizar.
#
# Args:
#   cur    → cursor de psycopg2
#   conn   → conexión de psycopg2 (para commit)
#   schema → "staging" o "core"
#   row    → dict con los datos actualizados del video
# ----------------------------------------------------------
def update_rows(cur, conn, schema, row):
    try:
        # Mapea los nombres de campo según el schema
        if schema == 'staging':
            # Staging usa los nombres de campo de la API de YouTube
            video_id       = 'video_id'
            upload_date    = 'publishedAt'
            video_title    = 'title'
            video_views    = 'viewCount'
            likes_count    = 'likeCount'
            comments_count = 'commentCount'
        else:
            # Core usa los nombres de campo estandarizados
            video_id       = 'Video_ID'
            upload_date    = 'Upload_Date'
            video_title    = 'Video_Title'
            video_views    = 'Video_Views'
            likes_count    = 'Likes_Count'
            comments_count = 'Comments_Count'

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title"    = %({video_title})s,
                "Video_Views"    = %({video_views})s,
                "Likes_Count"    = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID"   = %({video_id})s
              AND "Upload_Date" = %({upload_date})s;
            """, row
        )

        # Confirma la transacción
        conn.commit()

        logger.info(f"Updated row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e


# ----------------------------------------------------------
# FUNCIÓN: delete_rows
# ----------------------------------------------------------
# Elimina de la BD los videos que ya no existen en el canal.
#
# Recibe los IDs a eliminar y los borra en un solo query
# usando = ANY(array) — más eficiente que borrar uno por uno.
#
# ¿Por qué ANY(%s) y no armar un IN ('a', 'b') con texto?
#   Concatenar los IDs dentro del SQL permite SQL Injection.
#   Con el parámetro %s, psycopg2 adapta la lista de Python a un
#   array de Postgres y escapa cada valor de forma segura:
#     ids_list = ["abc", "def"]
#     → WHERE "Video_ID" = ANY(ARRAY['abc', 'def'])
#
# Args:
#   cur            → cursor de psycopg2
#   conn           → conexión de psycopg2 (para commit)
#   schema         → "staging" o "core"
#   ids_to_delete  → set o list de str con los Video_IDs a eliminar
#                    (se convierte a list porque psycopg2 no adapta sets)
# ----------------------------------------------------------
def delete_rows(cur, conn, schema, ids_to_delete):
    ids_list = list(ids_to_delete)
    try:
        # Query parametrizado: psycopg2 adapta la lista a un array de Postgres
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" = ANY(%s);
            """, (ids_list,)
        )

        # Confirma la transacción
        conn.commit()

        logger.info(f"Deleted rows with Video_IDs: {ids_list}")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {ids_list} - {e}")
        raise e
