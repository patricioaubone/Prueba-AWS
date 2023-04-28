import boto3
import pandas as pd

s3 = boto3.client("s3") #definimos un cliente para trabajar con S3 usando boto3
bucket_name = "bucket-udesa-prueba" #el nombre de nuestro bucket creado
s3_object = "advertiser_ids.csv" #el archivo que vamos a traernos



#Traemos de S3
obj = s3.get_object(Bucket = bucket_name, Key=s3_object) #definimos el archivo a levantar
df_advertiser_ids = pd.read_csv(obj['Body']) #levantamos el DF

print(df_advertiser_ids.head())





#Enviando a S3
desde_EC2 = pd.DataFrame.to_csv(df_advertiser_ids)
s3.put_object(Bucket=bucket_name, Key='desde_EC2.csv', Body=desde_EC2.encode('utf-8'))








#Enviando a RDS
import psycopg2
dbname = "basesql"
user = "postgres" #Configuracion / Disponibilidad / nombre de usuario maestro
password = "chavoLOCO23"
host = "database-1.c7mkdwca7kj0.us-east-1.rds.amazonaws.com" #Econectividad y seguridad
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

hacia_RDS = pd.DataFrame(df_advertiser_ids)

# Poblar la tabla con los datos del dataframe
for index, row in hacia_RDS.iterrows():
    cur.execute(f"INSERT INTO {tabla_RDS} (adv_id) VALUES (%s);", tuple(row))

# Confirmar los cambios
conn.commit()

# Cerrar la conexión
cur.close()
conn.close()



# Insertar los registros en la tabla 'table_name'
# table_name = 'tabla_RDS'
# hacia_RDS.to_sql(table_name, conn, if_exists='append', index=False)

# # Cerrar la conexión
# conn.close()



