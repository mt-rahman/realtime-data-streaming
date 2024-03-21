from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'mt-rahman',
    'start_date': datetime(2024, 3, 20, 10, 0)
}

def get_data():
    import json
    import requests

    response = requests.get('https://randomuser.me/api/')
    response = response.json()['results'][0]
    
    return response

def format_data(response):
    data = {}

    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = str(response['location']['street']['number']) + ' ' + response['location']['street']['name'] \
                        + ', ' + response['location']['city'] \
                        + ', ' + response['location']['state'] \
                        + ', ' + response['location']['country']
    data['postcode'] = response['location']['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time

    response = get_data()
    response = format_data(response)
    # print(json.dumps(response, indent=4))

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(response).encode('utf-8'))

with DAG(
        'user_automation',
        default_args=default_args,
        schedule='@daily',
        catchup=False
    ) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

if __name__ == "__main__":
    stream_data()