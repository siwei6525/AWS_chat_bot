import boto3
import datetime
import json
from botocore.vendored import requests
from urlparse import urljoin
from botocore.vendored import requests
from decimal import *
from time import sleep

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
TOKEN_PATH = '/oauth2/token'
GRANT_TYPE = 'client_credentials'

# Defaults for our simple example.
DEFAULT_LOCATION = 'Manhattan'
restaurants = {}

def search(cuisine, offset):
    url_params = {
        'location': DEFAULT_LOCATION,
        'offset': offset,
        'limit': 50,
        'term': cuisine + " restaurants",
        'sort_by': 'rating'
    }
    return request(API_HOST, SEARCH_PATH, url_params=url_params)


def request(host, path, url_params=None):
    url_params = url_params or {}
    url = urljoin(host, path)
    headers = {
        'Authorization': 'Bearer ccJUZkyk9IKTnziyN1PcSmqh56SJYcIsxzBLxLqIEL2innFP3s2Cx4izjPfBb2s47MBL7nKpl6y9BEn4Ao3mt6kfZ4YXEM7TAtZEJPfIf1L8JkwxozgbjhCE-smKXHYx',
    }

    response = requests.request('GET', url, headers=headers, params=url_params)
    rjson = response.json()
    return rjson


def addItems(data, cuisine):
    global restaurants
    with table.batch_writer() as batch:
        for rec in data:
            try:
                if rec["alias"] in restaurants:
                    print(11111111111);
                    continue;
                    
                restaurants[rec["alias"]] = 0
                rec["rating"] = Decimal(str(rec["rating"]))
                rec['cuisine'] = cuisine
                rec['insertedAtTimestamp'] = str(datetime.datetime.now())
                rec["coordinates"]["latitude"] = Decimal(str(rec["coordinates"]["latitude"]))
                rec["coordinates"]["longitude"] = Decimal(str(rec["coordinates"]["longitude"]))
                rec['address'] = rec['location']['display_address']
                rec.pop("distance", None)
                rec.pop("location", None)
                rec.pop("transactions", None)
                rec.pop("display_phone", None)
                rec.pop("categories", None)
                if rec["phone"] == "":
                    rec.pop("phone", None)
                if rec["image_url"] == "":
                    rec.pop("image_url", None)

                # print(rec)
                batch.put_item(Item=rec)
                sleep(0.001)
            except Exception as e:
                print(e)
                print(rec)

def scrape():
    cuisines = ['italian', 'chinese', 'indian', 'american', 'mexican', 'spanish', 'greek', 'latin', 'Persian']
    for cuisine in cuisines:
        offset = 0
        while offset < 800:
            js = search(cuisine, offset)
            addItems(js["businesses"], cuisine)
            offset += 50




def lambda_handler(event, context):
    scrape()
    
    print("success!!!!007")
    
    

    
    
    
    
    
    
    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
