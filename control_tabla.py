import psycopg2

# # Datos de conexión a la base de datos de PostgreSQL en RDS
# host = 'database-1.c7mkdwca7kj0.us-east-1.rds.amazonaws.com'
# database = 'basesql'
# user = 'postgres'
# password = 'chavoLOCO23'

# # Nombre de la tabla que quieres verificar
# table_name = 'tabla_RDS'

# # Conectar a la base de datos
# conn = psycopg2.connect(host=host, database=database, user=user, password=password)

# # Crear un cursor para ejecutar las consultas SQL
# cur = conn.cursor()

# # cur.execute("SELECT * FROM information_schema.table_privileges WHERE grantee = 'postgres';")
# # cur.execute("SELECT * FROM information_schema.role_table_grants WHERE table_name = 'tabla_RDS';")

# # Verificar si la tabla existe
# cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');")
# exists = cur.fetchone()[0]

# if exists:
#     print(f"La tabla {table_name} existe en la base de datos {database}.")
# else:
#     print(f"La tabla {table_name} no existe en la base de datos {database}.")

# # Cerrar la conexión
# cur.close()
# conn.close()


# Control conexión
try:
    conn = psycopg2.connect(
        host = 'database-1.c7mkdwca7kj0.us-east-1.rds.amazonaws.com',
        database='basesql',
        user='postgres',
        password='chavoLOCO23')
except Exception as e:
    print("Error de conexión:", e)
