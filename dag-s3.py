import boto3
import pandas as pd

s3 = boto3.client("s3") #definimos un cliente para trabajar con S3 usando boto3
bucket_name = "data-raw-udesa-prueba" #el nombre de nuestro bucket creado
s3_object = "advertiser_ids.csv" #el archivo que vamos a traernos

obj = s3.get_object(Bucket = bucket_name, Key=s3_object) #definimos el archivo a levantar

df_advertiser_ids = pd.read_csv(obj['Body']) #levantamos el DF
df_advertiser_ids.head()

# df_product_views = pd.read_csv(df_product_views)
# df_ads_views = pd.read_csv(df_ads_views)
