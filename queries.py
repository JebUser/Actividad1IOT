import psycopg2

# Configuración de PostgreSQL
DB_HOST = "localhost"
DB_NAME = "sensores"
DB_USER = "postgres"
DB_PASSWORD = "tu_password"

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

# Obtener el promedio de temperatura por dispositivo
def get_avg_temperature():
    conn = connect_db()
    if conn is None:
        return
    
    cursor = conn.cursor()
    query = """
        SELECT sensor_id, AVG(temperature) AS avg_temperature
        FROM sensor_data
        GROUP BY sensor_id;
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    print("Promedio de temperatura por dispositivo:")
    for row in results:
        print(f"Sensor: {row[0]}, Promedio de Temperatura: {row[1]:.2f}°C")
    
    cursor.close()
    conn.close()

# Obtener el número de mediciones por dispositivo
def get_measurement_count():
    conn = connect_db()
    if conn is None:
        return
    
    cursor = conn.cursor()
    query = """
        SELECT sensor_id, COUNT(*) AS total_measurements
        FROM sensor_data
        GROUP BY sensor_id;
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    print("Número de mediciones por dispositivo:")
    for row in results:
        print(f"Sensor: {row[0]}, Total Mediciones: {row[1]}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    get_avg_temperature()
    get_measurement_count()
