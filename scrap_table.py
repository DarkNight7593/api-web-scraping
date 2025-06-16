import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import json
import traceback

def lambda_handler(event, context):
    try:
        url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
        response = requests.get(url)
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': json.dumps({'error': 'No se pudo acceder a la página'})
            }

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        if not table:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No se encontró la tabla'})
            }

        headers = [header.text.strip() for header in table.find_all('th')]
        if not headers:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'La tabla no tiene encabezados'})
            }

        rows = []
        for tr in table.find_all('tr')[1:]:
            cells = tr.find_all('td')
            if not cells:
                continue
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.text.strip()
            rows.append(row_data)

        # 👉 Limitar a las primeras 10 filas
        rows = rows[:10]

        dynamodb = boto3.resource('dynamodb')
        tabla_dynamo = dynamodb.Table('TablaWebScrapping')

        # Eliminar todos los registros previos
        scan = tabla_dynamo.scan()
        with tabla_dynamo.batch_writer() as batch:
            for item in scan['Items']:
                batch.delete_item(Key={'id': item['id']})

        # Insertar solo las 10 primeras filas
        for i, row in enumerate(rows, start=1):
            row['#'] = i
            row['id'] = str(uuid.uuid4())
            tabla_dynamo.put_item(Item=row)

        return {
            'statusCode': 200,
            'body': json.dumps(rows, ensure_ascii=False)
        }

    except Exception as e:
        error_log = {
            'error': str(e),
            'traceback': traceback.format_exc()
        }
        print(json.dumps(error_log))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
