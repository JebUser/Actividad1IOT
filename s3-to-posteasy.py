import boto3
import json
import psycopg2
from io import BytesIO

# Configuración de AWS S3
BUCKET_NAME = "awssensorsbucket"
REGION = "us-east-1"

# Configuración de PostgreSQL
DB_HOST = "34.207.143.199"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "YourNewPassword"

# Conectar a PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {e}")
        return None

# Leer archivos desde S3 y cargarlos a PostgreSQL
def load_data_from_s3():
    s3 = boto3.client('s3', region_name=REGION)
    conn = connect_db()
    if conn is None:
        return
    
    cursor = conn.cursor()
    
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        if 'Contents' not in response:
            print("No hay archivos en el bucket.")
            return
        
        for obj in response['Contents']:
            file_name = obj['Key']
            print(f"Procesando archivo: {file_name}")
            
            # Descargar el archivo
            file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
            file_content = file_obj['Body'].read()
            data = json.loads(file_content)
            
            for record in data:
                cursor.execute(
                    """
                    INSERT INTO sensor_data (sensor_id, timestamp, temperature, humidity, latitude, longitude, battery_level)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        record["sensor_id"],
                        record["timestamp"],
                        record["temperature"],
                        record["humidity"],
                        record["location"]["latitude"],
                        record["location"]["longitude"],
                        record["battery_level"]
                    )
                )
        
        conn.commit()
        print("Datos cargados exitosamente en la base de datos.")
    except Exception as e:
        print(f"Error cargando datos: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_data_from_s3()
