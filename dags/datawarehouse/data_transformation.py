# =============================================================================
# data_transformation.py
# =============================================================================
# Propósito:
#   Transformar los datos crudos que vienen de la API de YouTube antes de
#   cargarlos a la base de datos Postgres.
#
# Transformaciones que se aplican a cada fila (video):
#   1. Convertir la duración ISO 8601 (ej: "PT1H23M45S") a un objeto time de Python.
#   2. Clasificar el video como "Shorts" (≤ 60 seg) o "Normal" (> 60 seg).
#
# Flujo en el pipeline:
#   staging (Postgres)  →  transforme_data()  →  insert/update en schema core
# =============================================================================

from datetime import timedelta
from datetime import datetime


# -----------------------------------------------------------------------------
# parse_duration(duration_str)
# -----------------------------------------------------------------------------
# Convierte una duración en formato ISO 8601 (como la devuelve la API de YouTube)
# a un objeto timedelta de Python que representa una duración real.
#
# Ejemplo:
#   "PT1H23M45S"  →  timedelta(hours=1, minutes=23, seconds=45)
#   "PT45S"       →  timedelta(seconds=45)
#   "P1DT2H"      →  timedelta(days=1, hours=2)
# -----------------------------------------------------------------------------
def parse_duration(duration_str):

    # Paso 1: Eliminar los separadores del formato ISO 8601
    # "P" marca el inicio de una duración, "T" separa días de horas/minutos/segundos
    # Ejemplo: "PT1H23M45S" → "1H23M45S"
    duration_str = duration_str.replace("P","").replace("T","")

    # Paso 2: Definir los componentes de tiempo y sus valores por defecto (0)
    # El orden importa: se procesa de mayor a menor unidad (D → H → M → S)
    components = ["D","H","M","S"]
    values = {"D":0,"H":0,"M":0,"S":0}

    # Paso 3: Extraer el valor numérico de cada componente presente en el string
    # Truco clave: split(component) divide el string en dos partes:
    #   - La izquierda (value) → número antes del componente
    #   - La derecha (duration_str) → el resto del string aún sin procesar
    # Ejemplo con "1H23M45S":
    #   split("H") → ["1", "23M45S"]  →  value="1", duration_str="23M45S"
    #   split("M") → ["23", "45S"]    →  value="23", duration_str="45S"
    #   split("S") → ["45", ""]       →  value="45", duration_str=""
    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    # Paso 4: Construir el objeto timedelta con los valores extraídos
    # timedelta es la clase de Python que representa una duración de tiempo
    # Permite operaciones matemáticas como sumar, restar y comparar duraciones
    total_duration = timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )
    
    return total_duration


# -----------------------------------------------------------------------------
# transforme_data(row)
# -----------------------------------------------------------------------------
# Aplica todas las transformaciones necesarias a una fila (dict) de datos de video
# antes de insertarla en la base de datos Postgres.
#
# Argumento:
#   row (dict) → un video con todos sus campos tal como vienen de extract_video_data()
#
# Transformaciones:
#   1. row['Duration'] → convierte "PT1H23M45S" a un objeto time (ej: 01:23:45)
#   2. row['Video_Type'] → clasifica el video como "Shorts" o "Normal"
#      (con T mayúscula: debe coincidir con la columna "Video_Type" de core
#       y con %(Video_Type)s del INSERT en data_modification.py)
#
# Retorna:
#   row (dict) → el mismo diccionario con los campos transformados
# -----------------------------------------------------------------------------
def transforme_data(row):

    # Paso 1: Convertir la duración ISO 8601 a timedelta para poder operar con ella
    duration_td = parse_duration(row['Duration'])

    # Paso 2: Convertir timedelta a un objeto time de Python (HH:MM:SS)
    # datetime.min es el datetime más pequeño posible (0001-01-01 00:00:00)
    # Al sumarle el timedelta obtenemos un datetime con la duración
    # .time() extrae solo la parte de hora, descartando la fecha
    # Ejemplo: timedelta(hours=1, minutes=23, seconds=45) → time(1, 23, 45)
    row['Duration'] = (datetime.min + duration_td).time()

    # Paso 3: Clasificar el video como "Shorts" o "Normal"
    # YouTube Shorts son videos de 60 segundos o menos
    # total_seconds() convierte el timedelta completo a segundos (ej: 83.0)
    row['Video_Type'] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row
