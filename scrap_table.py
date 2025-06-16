import requests
import boto3
import uuid
import json

def lambda_handler(event, context):
    # URL de la API del IGP que contiene los datos de sismos
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"
    
    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Obtener los datos en formato JSON
    datos = response.json()
    
    # Obtener los últimos 10 registros y ordenarlos del más reciente al más antiguo
    ultimos = datos[-10:]
    ultimos.reverse()

    # Conectarse a DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')

    # Eliminar todos los elementos existentes en la tabla
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    # Insertar los nuevos datos
    for i, row in enumerate(ultimos, start=1):
        row['#'] = i
        row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
        table.put_item(Item=row)

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': json.dumps(ultimos, ensure_ascii=False)
    }
