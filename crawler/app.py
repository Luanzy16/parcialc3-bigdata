import boto3

def handler(event, context):
    glue = boto3.client('glue')
    
    crawler_name = "parcial3"  # Cambia esto al nombre de tu crawler Glue

    try:
        response = glue.start_crawler(Name=crawler_name)
        print(f"Crawler {crawler_name} iniciado correctamente.")
        return {
            "statusCode": 200,
            "body": f"Crawler {crawler_name} iniciado."
        }
    except glue.exceptions.CrawlerRunningException:
        # Si el crawler ya está corriendo
        print(f"Crawler {crawler_name} ya está corriendo.")
        return {
            "statusCode": 200,
            "body": f"Crawler {crawler_name} ya está corriendo."
        }
    except Exception as e:
        print(f"Error iniciando el crawler {crawler_name}: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
