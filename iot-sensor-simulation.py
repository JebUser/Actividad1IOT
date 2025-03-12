import boto3
import json
import random
import time
import os
from datetime import datetime, timedelta

# Configuración de sensores
SENSOR_COUNT = 5  # Número de sensores a simular
MEASUREMENTS_PER_FILE = 10  # Mediciones por archivo JSON
FILES_TO_GENERATE = 5  # Número de archivos JSON a generar
TIME_INTERVAL = 60  # Intervalo máximo entre mediciones (segundos)
FILE_INTERVAL = 2  # Intervalo entre archivos JSON (segundos) - reducido para pruebas locales

# Configuración de AWS S3 (solo se usará si decides subir a S3)
BUCKET_NAME = "awssensorsbucket"  # Reemplazar con tu nombre de bucket
REGION = "us-east-1"  # Reemplazar con tu región
UPLOAD_TO_S3 = True  # Cambiar a True cuando quieras subir a S3

# Directorio para guardar los archivos JSON
OUTPUT_DIR = "sensor_data"

# Configuración de límites para datos aleatorios
TEMP_MIN, TEMP_MAX = 15.0, 35.0  # Rango de temperatura en grados Celsius
HUMIDITY_MIN, HUMIDITY_MAX = 30.0, 90.0  # Rango de humedad en porcentaje
BATTERY_MIN, BATTERY_MAX = 80, 100  # Rango de nivel de batería

# Ubicaciones fijas para cada sensor (coordenadas en San Francisco y alrededores)
LOCATIONS = [
    {"latitude": 37.7749, "longitude": -122.4194},  # San Francisco
    {"latitude": 37.7831, "longitude": -122.4039},  # Área financiera
    {"latitude": 37.8085, "longitude": -122.4097},  # Fisherman's Wharf
    {"latitude": 37.7608, "longitude": -122.4286},  # Mission District
    {"latitude": 37.8029, "longitude": -122.4408}   # Golden Gate Park
]

def initialize_s3_client():
    """Inicializa y retorna un cliente S3."""
    print("Inicializando cliente S3...")
    try:
        # Intenta usar credenciales configuradas (archivo ~/.aws/credentials o variables de entorno)
        s3_client = boto3.client('s3', region_name=REGION)
        return s3_client
    except Exception as e:
        print(f"Error al inicializar cliente S3: {e}")
        return None

def create_bucket_if_not_exists(s3_client):
    """Crea el bucket S3 si no existe."""
    try:
        # Verificar si el bucket ya existe
        response = s3_client.list_buckets()
        bucket_exists = any(bucket['Name'] == BUCKET_NAME for bucket in response['Buckets'])
        
        if not bucket_exists:
            print(f"Creando bucket S3: {BUCKET_NAME}")
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' creado exitosamente.")
        else:
            print(f"El bucket '{BUCKET_NAME}' ya existe.")
        return True
    except Exception as e:
        print(f"Error al crear o verificar bucket: {e}")
        return False

def generate_sensor_data(sensor_id, current_time):
    """Genera datos aleatorios para un sensor específico."""
    # Obtener ubicación fija para este sensor
    location_index = int(sensor_id.split('-')[1]) % len(LOCATIONS)
    location = LOCATIONS[location_index]
    
    # Generar datos aleatorios para temperatura, humedad y batería
    temperature = round(random.uniform(TEMP_MIN, TEMP_MAX), 1)
    humidity = round(random.uniform(HUMIDITY_MIN, HUMIDITY_MAX), 1)
    battery_level = random.randint(BATTERY_MIN, BATTERY_MAX)
    
    # Formatear timestamp en ISO 8601
    timestamp = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Crear y retornar objeto de medición
    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity,
        "location": location,
        "battery_level": battery_level
    }

def generate_measurements_file(file_number):
    """Genera un archivo JSON con múltiples mediciones de diferentes sensores."""
    # Crear directorio de salida si no existe
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Directorio {OUTPUT_DIR} creado.")
    
    measurements = []
    base_time = datetime.now()
    
    # Generar mediciones para diferentes sensores
    for i in range(MEASUREMENTS_PER_FILE):
        # Seleccionar un sensor aleatorio
        sensor_id = f"THS-{str(random.randint(1, SENSOR_COUNT)).zfill(3)}"
        
        # Calcular tiempo para esta medición
        measurement_time = base_time + timedelta(seconds=i * (TIME_INTERVAL / MEASUREMENTS_PER_FILE))
        
        # Generar y añadir medición
        measurement = generate_sensor_data(sensor_id, measurement_time)
        measurements.append(measurement)
    
    # Crear objeto JSON con todas las mediciones
    data = {"measurements": measurements}
    
    # Escribir a archivo en el directorio de salida
    filename = os.path.join(OUTPUT_DIR, f"sensor_data_{file_number}.json")
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Archivo {filename} generado con {len(measurements)} mediciones.")
    return filename

def upload_file_to_s3(s3_client, filename):
    """Sube un archivo al bucket S3."""
    if s3_client is None:
        print("Cliente S3 no inicializado. No se puede subir el archivo.")
        return False
    
    try:
        object_name = os.path.basename(filename)
        print(f"Subiendo {filename} a S3...")
        s3_client.upload_file(filename, BUCKET_NAME, object_name)
        print(f"Archivo {filename} subido exitosamente a '{BUCKET_NAME}/{object_name}'")
        return True
    except Exception as e:
        print(f"Error al subir archivo a S3: {e}")
        return False

def main():
    """Función principal que ejecuta la simulación y opcionalmente carga de datos."""
    # Usar la variable global UPLOAD_TO_S3, no crear una nueva variable local
    global UPLOAD_TO_S3
    
    # Inicializar cliente S3 solo si vamos a subir archivos
    s3_client = initialize_s3_client() if UPLOAD_TO_S3 else None
    
    # Crear bucket si no existe (solo si vamos a subir archivos)
    if UPLOAD_TO_S3 and s3_client is not None:
        if not create_bucket_if_not_exists(s3_client):
            print("No se pudo crear/verificar el bucket. Continuando sin subir a S3.")
            UPLOAD_TO_S3 = False  # Aquí es donde necesitamos la declaración global
    
    # Lista para almacenar nombres de archivos generados
    generated_files = []
    
    # Generar archivos y opcionalmente subirlos
    for i in range(1, FILES_TO_GENERATE + 1):
        # Generar archivo con mediciones
        filename = generate_measurements_file(i)
        generated_files.append(filename)
        
        # Subir archivo a S3 si está habilitado
        if UPLOAD_TO_S3:
            uploaded = upload_file_to_s3(s3_client, filename)
            if not uploaded:
                print("Ocurrió un error al subir el archivo. Continuando con el siguiente...")
        
        # Esperar antes de generar el siguiente archivo (excepto el último)
        if i < FILES_TO_GENERATE:
            print(f"Esperando {FILE_INTERVAL} segundos antes de generar el siguiente archivo...")
            time.sleep(FILE_INTERVAL)
    
    print("\nResumen:")
    print(f"Se generaron {FILES_TO_GENERATE} archivos JSON en el directorio '{OUTPUT_DIR}'")
    if UPLOAD_TO_S3:
        print(f"Los archivos fueron subidos al bucket S3 '{BUCKET_NAME}'")
    print("Proceso completado exitosamente.")
    
    # Mostrar estructura de un archivo para verificación
    print("\nEstructura de ejemplo (primer archivo):")
    if generated_files:
        with open(generated_files[0], 'r') as f:
            data = json.load(f)
            # Mostrar solo la primera medición para no saturar la salida
            print(json.dumps({"measurements": [data["measurements"][0], "..."]}, indent=2))
            print(f"El archivo contiene {len(data['measurements'])} mediciones en total.")

if __name__ == "__main__":
    main()
