import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import json
import random
from boto3.dynamodb.conditions import Key


# es:
credentials = boto3.Session(aws_access_key_id='AKIAJTEPZWQC2BNRJOGQ',
                            aws_secret_access_key='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').get_credentials()
service = 'es'
region = 'us-east-1'
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
host = 'search-restaurants-nvtlhnftwpkzs2ynppcz4x3dg4.us-east-1.es.amazonaws.com'
es = Elasticsearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

# db:
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')


def pollSNS():
    # create a boto3 client
    client = boto3.client('sqs')
    sms_client = boto3.client('sns')

    # create the test queue
    # for a FIFO queue, the name must end in .fifo, and you must pass FifoQueue = True
    # client.create_queue(QueueName='dinningQueue')
    # get a list of queues, we get back a dict with 'QueueUrls' as a key with a list of queue URLs
    queues = client.list_queues(QueueNamePrefix='chat-bot')  # we filter to narrow down the list
    test_queue_url = queues['QueueUrls'][0]

    while True:
        # Receive message from SQS queue
        response = client.receive_message(
            QueueUrl=test_queue_url,
            AttributeNames=[
                'All'
            ],
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )

        if 'Messages' in response:  # when the queue is exhausted, the response dict contains no 'Messages' key
            # print ("total message: ", len(response['Messages']))

            for message in response['Messages']:  # 'Messages' is a list
                js = json.loads(message['Body'])
                cuisine = js['cuisine']
                email = js['email']
                phone = js['phone']
                query = {'query': {'term': {'Cuisine': cuisine}}}
                allDoc = es.search(index='restaurants', doc_type='Restaurant', body=query)
                n_vals = allDoc['hits']["total"]["value"]
                idx = random.randint(0, 9)
                # print(idx)
                # print(len(allDoc['hits']['hits']))
                res = allDoc['hits']['hits'][idx]['_source']['RestaurantID']
                # print(res)
                

                dbRes = table.query(KeyConditionExpression=Key('insertedAtTimestamp').eq(res))

                client.delete_message(QueueUrl=test_queue_url, ReceiptHandle=message['ReceiptHandle'])

                if 'price' in dbRes['Items'][0].keys():
                    price = str(dbRes['Items'][0]['price'])
                else:
                    price = 'NA'
                if 'phone' in dbRes['Items'][0].keys():
                    restaurant_phone = str(dbRes['Items'][0]['phone'])
                else:
                    restaurant_phone = 'NA'
                addr = str(dbRes['Items'][0]['address'])
                for char in "'u[]":
                    addr = addr.replace(char, '')
                message = 'This is an ideal(random) deal for you:\n' + 'Restaurant Name: ' + dbRes['Items'][0][
                    'name'] + '\n' + 'Price: ' + price + '\n' + 'Phone: ' + restaurant_phone + '\n' + 'Address: ' + addr + '\n' + 'Enjoy your meal!'
                check = sms_client.publish(PhoneNumber=str(phone), Message=message)

                print(str(check))

                # print(phone)
                # print(message)

        else:
            print('Queue is now empty')
            break


def lambda_handler(event, context):
    pollSNS()


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }