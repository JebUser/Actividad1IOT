import boto3
import json
import psycopg2
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"s3_to_postgres_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("s3_to_postgres")

# Cargar variables de entorno (alternativa más segura)
load_dotenv()

# Configuración desde variables de entorno
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "awssensorsbucket")
REGION = os.getenv("AWS_REGION", "us-east-1")
DB_HOST = os.getenv("DB_HOST", "34.207.143.199")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "YourNewPassword")
DB_SCHEMA = os.getenv("DB_SCHEMA", "sensors")  # Nuevo: variable para el esquema
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # Procesar en lotes para mejor rendimiento

class S3ToPostgresLoader:
    def __init__(self):
        self.s3_client = boto3.client('s3', region_name=REGION)
        self.conn = None
        self.cursor = None
    
    def connect_db(self):
        """Establece conexión con la base de datos PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=10
            )
            self.cursor = self.conn.cursor()
            
            # Asegurar que el esquema existe
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
            self.conn.commit()
            
            logger.info(f"Conexión exitosa a la base de datos. Esquema {DB_SCHEMA} verificado.")
            return True
        except Exception as e:
            logger.error(f"Error conectando a PostgreSQL: {e}")
            return False
    
    def check_table_exists(self):
        """Verifica si la tabla existe y la crea si no existe"""
        try:
            self.cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{DB_SCHEMA}'
                AND table_name = 'sensor_data'
            )
            """)
            
            exists = self.cursor.fetchone()[0]
            
            if not exists:
                logger.info(f"La tabla {DB_SCHEMA}.sensor_data no existe. Creándola...")
                self.cursor.execute(f"""
                CREATE TABLE {DB_SCHEMA}.sensor_data (
                    id SERIAL PRIMARY KEY,
                    sensor_id VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    temperature FLOAT NOT NULL,
                    humidity FLOAT NOT NULL,
                    latitude DECIMAL(9,6) NOT NULL,
                    longitude DECIMAL(9,6) NOT NULL,
                    battery_level INT NOT NULL
                )
                """)
                self.conn.commit()
                logger.info(f"Tabla {DB_SCHEMA}.sensor_data creada exitosamente.")
            else:
                logger.info(f"Tabla {DB_SCHEMA}.sensor_data ya existe.")
                
            return True
        except Exception as e:
            logger.error(f"Error verificando/creando tabla: {e}")
            return False
    
    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Conexión a la base de datos cerrada")
    
    def get_s3_files(self):
        """Obtiene la lista de archivos en el bucket S3"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=BUCKET_NAME)
            
            files = []
            for page in page_iterator:
                if 'Contents' in page:
                    files.extend([obj['Key'] for obj in page['Contents']])
            
            logger.info(f"Se encontraron {len(files)} archivos en el bucket S3")
            return files
        except Exception as e:
            logger.error(f"Error obteniendo archivos de S3: {e}")
            return []
    
    def download_and_parse_file(self, file_name):
        """Descarga y parsea un archivo JSON de S3"""
        try:
            logger.info(f"Descargando archivo: {file_name}")
            file_obj = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=file_name)
            file_content = file_obj['Body'].read()
            data = json.loads(file_content)
            logger.info(f"Archivo {file_name} parseado con {len(data)} registros")
            return data
        except Exception as e:
            logger.error(f"Error procesando archivo {file_name}: {e}")
            return []
    
    def process_file(self, file_name):
        """Procesa un archivo y carga sus datos en PostgreSQL"""
        data = self.download_and_parse_file(file_name)
        if not data:
            return 0
        
        records_to_insert = []
        for record in data:
            try:
                # Validar que los campos obligatorios existan
                if not all(key in record for key in ["sensor_id", "timestamp", "temperature", "humidity", "location", "battery_level"]):
                    logger.warning(f"Registro incompleto: {record}")
                    continue
                
                # Validar que location contenga latitude y longitude
                if not all(key in record["location"] for key in ["latitude", "longitude"]):
                    logger.warning(f"Datos de ubicación incompletos: {record}")
                    continue
                
                # Convertir datos a los tipos correctos según la definición de la tabla
                sensor_id = str(record["sensor_id"])
                
                # Convertir timestamp a formato PostgreSQL
                try:
                    timestamp = record["timestamp"]
                    # Si es un string, asumimos formato ISO
                    if isinstance(timestamp, str):
                        timestamp = timestamp
                except:
                    logger.warning(f"Error en formato de timestamp: {record['timestamp']}")
                    continue
                
                # Convertir temperatura y humedad a float
                try:
                    temperature = float(record["temperature"])
                    humidity = float(record["humidity"])
                except:
                    logger.warning(f"Error en temperatura/humedad: {record}")
                    continue
                
                # Convertir coordenadas a decimal con precisión correcta
                try:
                    latitude = float(record["location"]["latitude"])
                    longitude = float(record["location"]["longitude"])
                    # Verificar que estén en rangos válidos
                    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                        logger.warning(f"Coordenadas fuera de rango: {latitude}, {longitude}")
                        continue
                except:
                    logger.warning(f"Error en coordenadas: {record['location']}")
                    continue
                
                # Convertir nivel de batería a entero
                try:
                    battery_level = int(record["battery_level"])
                except:
                    logger.warning(f"Error en nivel de batería: {record['battery_level']}")
                    continue
                
                records_to_insert.append((
                    sensor_id,
                    timestamp,
                    temperature,
                    humidity,
                    latitude,
                    longitude,
                    battery_level
                ))
            except Exception as e:
                logger.error(f"Error procesando registro: {e}, registro: {record}")
        
        # Insertar en lotes para mejor rendimiento
        if records_to_insert:
            try:
                # Referencia a la tabla con el esquema
                query = f"""
                INSERT INTO {DB_SCHEMA}.sensor_data 
                (sensor_id, timestamp, temperature, humidity, latitude, longitude, battery_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                execute_batch(self.cursor, query, records_to_insert, page_size=BATCH_SIZE)
                self.conn.commit()
                logger.info(f"Se insertaron {len(records_to_insert)} registros desde {file_name}")
                return len(records_to_insert)
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Error insertando datos: {e}")
                return 0
        return 0
    
    def mark_file_as_processed(self, file_name):
        """Opcionalmente, mueve o marca el archivo como procesado"""
        try:
            # Opción 1: Mover a otra carpeta "processed"
            new_key = f"processed/{file_name.split('/')[-1]}"
            self.s3_client.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={'Bucket': BUCKET_NAME, 'Key': file_name},
                Key=new_key
            )
            # Opcionalmente, borrar el original
            # self.s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_name)
            logger.info(f"Archivo {file_name} marcado como procesado")
        except Exception as e:
            logger.warning(f"No se pudo marcar el archivo como procesado: {e}")
    
    def load_data_from_s3(self):
        """Carga todos los datos desde S3 a PostgreSQL"""
        if not self.connect_db():
            return
        
        try:
            # Verificar y crear tabla si no existe
            if not self.check_table_exists():
                return
                
            total_processed = 0
            files = self.get_s3_files()
            
            if not files:
                logger.warning("No se encontraron archivos para procesar")
                return
            
            for file_name in files:
                records_processed = self.process_file(file_name)
                total_processed += records_processed
                
                # Opcionalmente, marcar el archivo como procesado
                if records_processed > 0:
                    self.mark_file_as_processed(file_name)
            
            logger.info(f"Proceso completado. Total de registros procesados: {total_processed}")
        except Exception as e:
            logger.error(f"Error en el proceso de carga: {e}")
        finally:
            self.close_connection()

def main():
    """Función principal"""
    try:
        logger.info("Iniciando proceso de carga S3 a PostgreSQL")
        loader = S3ToPostgresLoader()
        loader.load_data_from_s3()
        logger.info("Proceso finalizado")
    except Exception as e:
        logger.error(f"Error en la ejecución principal: {e}")

if __name__ == "__main__":
    main()