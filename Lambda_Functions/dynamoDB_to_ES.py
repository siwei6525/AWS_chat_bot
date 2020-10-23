import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

credentials = boto3.Session(aws_access_key_id='AKIAJTEPZWQC2BNRJOGQ',
                            aws_secret_access_key='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').get_credentials()

service = 'es'
region = 'us-east-1'
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'search-restaurants-nvtlhnftwpkzs2ynppcz4x3dg4.us-east-1.es.amazonaws.com'

print(awsauth)

es = Elasticsearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


# 创建
dynamodb = boto3.resource('dynamodb', region_name=region, aws_access_key_id=credentials.access_key,
                          aws_secret_access_key=credentials.secret_key)
table = dynamodb.Table('yelp-restaurants')
resp = table.scan()
i = 0
while True:
    for item in resp['Items']:
        body = {"RestaurantID": item['insertedAtTimestamp'], "Cuisine": item['cuisine']}
        id = body["RestaurantID"]

        es.index(index="restaurants", doc_type="Restaurant", id=body['RestaurantID'], body=body)
        i += 1

    if 'LastEvaluatedKey' in resp:
        resp = table.scan(
            ExclusiveStartKey=resp['LastEvaluatedKey']
        )
        # break;
    else:
        break;

print("aaaaaaaaaaaaaaa")
print(i)



# # 删除
# es.indices.delete(index='restaurants', ignore=[400, 404])



# 测试 查询
# print(es.get(index="restaurants", doc_type="Restaurant", id="2020-10-22 03:49:03.070445"))
# print(es.get(index="restaurants", doc_type="Restaurant", id="2022-10-22 03:49:03.070445"))
# print(es.get(index="test", doc_type="Restaurant", id=r['RestaurantID']))
# print(es.get(index="test", doc_type="Restaurant", id=r2['RestaurantID']))
