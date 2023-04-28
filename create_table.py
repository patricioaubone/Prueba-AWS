import psycopg2
dbname = "basesql"
user = "postgres" #Configuracion / Disponibilidad / nombre de usuario maestro
password = "chavoLOCO23"
host = "database-1.c7mkdwca7kj0.us-east-1.rds.amazonaws.com" #Conectividad y seguridad
port = "5432"

#Creamos la conexión a RDS
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS tabla_RDS (adv_id VARCHAR(50));""")
conn.commit()

# Cerrar la conexión
cur.close()
conn.close()







