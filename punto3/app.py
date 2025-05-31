import boto3
from botocore.exceptions import ClientError

# Nombre del crawler que quieres ejecutar
CRAWLER_NAME = 'parcial3'

def handler(crawler_name):
    glue_client = boto3.client('glue')
    
    try:
        # Inicia el crawler
        glue_client.start_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' iniciado exitosamente.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'CrawlerRunningException':
            print(f"El crawler '{crawler_name}' ya está en ejecución.")
        else:
            print(f"Error al ejecutar el crawler: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

if __name__ == '__main__':
    handler(CRAWLER_NAME)
