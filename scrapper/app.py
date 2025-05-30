import requests
import boto3
from datetime import datetime

# --- Configuración S3 ---
S3_BUCKET_NAME = "parcial3luis" # ¡CAMBIA ESTO por tu bucket real!
S3_PREFIX = "headlines/raw" # Prefijo para la estructura de carpetas en S3
s3 = boto3.client('s3')

def upload_to_s3(file_content, object_name):
    """Sube el contenido de la página a S3."""
    try:
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=object_name, Body=file_content, ContentType='text/html')
        print(f"Archivo subido a S3: s3://{S3_BUCKET_NAME}/{object_name}")
        return True
    except Exception as e:
        print(f"Error al subir a S3: {e}")
        return False

def download_and_save_page(url, site_name):
    """Descarga la página web y la sube a S3 con la estructura deseada."""
    try:
        response = requests.get(url)
        response.raise_for_status() # Lanza una excepción para errores HTTP (ej. 404, 500)

        # Formato de fecha para el nombre del archivo
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Define el nombre del objeto en S3 según la estructura solicitada
        object_name = f"{S3_PREFIX}/{site_name}-{date_str}.html"

        return upload_to_s3(response.text, object_name)

    except requests.exceptions.RequestException as e:
        print(f"Error al descargar {url}: {e}")
        return False

def handler(event, context):
    """
    Función principal que será ejecutada por AWS Lambda.
    """
    print("Iniciando descarga y subida de páginas a S3...")
    
    # Descargar y guardar El Tiempo
    success_tiempo = download_and_save_page("https://www.eltiempo.com/", "eltiempo")
    
    # Descargar y guardar El Espectador
    success_espectador = download_and_save_page("https://www.elespectador.com/", "elespectador")
    
    # Opcional: Descargar y guardar Publimetro (descomenta si lo prefieres)
    # success_publimetro = download_and_save_page("https://www.publimetro.co/", "publimetro")

    if success_tiempo and success_espectador: # y success_publimetro si lo incluyes
        return {
            'statusCode': 200,
            'body': 'Páginas descargadas y subidas a S3 exitosamente.'
        }
    else:
        return {
            'statusCode': 500,
            'body': 'Hubo un error al descargar y/o subir una o más páginas.'
        }

if __name__ == '__main__':
    # Para probar la función localmente (requiere que tengas configuradas tus credenciales AWS
    # y que el bucket S3_BUCKET_NAME ya exista).
    print("Ejecutando localmente...")
    handler(None, None)
    print("Ejecución local terminada.")