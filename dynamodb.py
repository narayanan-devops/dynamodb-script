import boto3
import requests
import json
import logging
import sys
import os
from pprint import pprint
from zipfile import ZipFile
from decimal import Decimal
from io import BytesIO
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

#initilize
dynamodb = boto3.resource('dynamodb')
logger = logging.getLogger(__name__)
max_choices=7

def get_sample_movie_data():
    print(f"Downloading movies data ...")
    movie_content = requests.get(
            'https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/samples/moviedata.zip')
    movie_zip = ZipFile(BytesIO(movie_content.content))
    movie_zip.extractall()

    with open('moviedata.json') as movie_file:
        movie_data = json.load(movie_file, parse_float=Decimal)
        return movie_data[:100]

def check_table_exists(table_name):
    # Instantiate your dynamo client object
    client = boto3.client('dynamodb')
    response = client.list_tables()
    if table_name in response['TableNames']:
        return True
    else:
        return False

def create_table(table_name):
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                    {'AttributeName': 'year', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'title', 'KeyType': 'RANGE'}  # Sort key
            ],
            AttributeDefinitions=[
                    {'AttributeName': 'year', 'AttributeType': 'N'},
                    {'AttributeName': 'title', 'AttributeType': 'S'}
         ],
            ProvisionedThroughput={
             'ReadCapacityUnits': 100,
             'WriteCapacityUnits': 100
            }
        )
        table.wait_until_exists()
    except ClientError as err:
            logger.error(
              "Couldn't create table %s. Here's why: %s: %s", table_name,
               err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    else:
        print(f"Table {table_name} created")
        return table

def write_batch(movies):
    table = dynamodb.Table('Movies')
    try:
        with table.batch_writer() as writer:
            for movie in movies:
                writer.put_item(Item=movie)
    except ClientError as err:
        logger.error(
            "Couldn't load data into table %s. Here's why: %s: %s", table.name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        print(f"Data loaded into {table.name} ")

def add_movie(table_name, title, year, plot, rating):
        table = dynamodb.Table(table_name)
        try:
            table.put_item(
                Item={
                    'year': year,
                    'title': title,
                    'info': {'plot': plot, 'rating': Decimal(str(rating))}})
        except ClientError as err:
            logger.error(
                "Couldn't add movie %s to table %s. Here's why: %s: %s",
                title, table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            print(f"\n \n Movie {title} added")

def get_movie(table_name, title, year):
    table = dynamodb.Table(table_name)
    try:
        response = table.get_item(Key={'year': year, 'title': title})
    except ClientError as err:
        logger.error(
            "Couldn't get movie %s from table %s. Here's why: %s: %s",
            title, table_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['Item']

def query_movies(table_name, year):
    table = dynamodb.Table(table_name)
    try:
        response = table.query(KeyConditionExpression=Key('year').eq(year))
    except ClientError as err:
        logger.error(
            "Couldn't query for movies released in %s. Here's why: %s: %s", year,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['Items']

def scan_movies(table_name, year_range):
    table = dynamodb.Table(table_name)
    movies = []
    scan_kwargs = {
            'FilterExpression': Key('year').between(year_range['first'], year_range['second']),
            'ProjectionExpression': "#yr, title, info.rating",
            'ExpressionAttributeNames': {"#yr": "year"}}
    try:
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            movies.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
    except ClientError as err:
            logger.error(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    return movies


def checkinput(num):
   if num.isdigit() and range(1, max_choices):
      return True
   else:
      return False


while True:
    input_choice = input("\n \n Do you want to: \n 1) CREATE_TABLE \n 2) ADD_SAMPLE_MOVIES_DATA(Uses batch writer) \n 3) ADD_MOVIE(Uses PUT item)  \n 4) GET_MOVIE(Use GET item) \n 5) QUERY_MOVIES(Uses Query) \n 6) SCAN_MOVIES(Uses Scan) \n 7) QUIT \n Enter your choice: ")
    if checkinput(input_choice):
        if input_choice == "1":
            table_name = str(input("Enter table name to create(Default: Movies): ") or "Movies")
            if check_table_exists(table_name) == False:
                print ("=*="*10)
                print("\n \n")
                create_table(table_name)
                print("\n \n")
                print ("=*="*10)
            else:
                print ("=*="*10)
                print("\n \n")
                print(f"Table {table_name} already exists")
                print("\n \n")
                print ("=*="*10)
        elif input_choice == "2":
            movie_data = get_sample_movie_data()
            write_batch(movie_data)
        elif input_choice == "3":
            movie_name = str(input("Enter movie name to add: "))
            movie_year = int(input("Enter movie year: "))
            movie_plot = str(input("Enter movie plot: "))
            movie_rating = int(input("Enter movie rating: "))
            add_movie(table_name, movie_name , movie_year, movie_plot, movie_rating)
        elif input_choice == "4":
            movie_name = str(input("Enter movie name to get: "))
            movie_year = int(input("Enter movie year: "))
            result_get_movie = get_movie(table_name, movie_name, movie_year)
            print("\n \n")
            print ("=*="*10)
            print(result_get_movie)
            print ("=*="*10)
        elif input_choice == "5":
            get_movie_by_year = int(input("Enter the year to find movies released during in that year: "))
            result_query_movies=query_movies(table_name, get_movie_by_year)
            print ("=*="*10)
            print ("Movies released in %s" % get_movie_by_year)
            print ("=*="*10)
            for i in result_query_movies:
                print (i['title'])
        elif input_choice == "6":
            movie_year_range = {'first': int(input("Enter first year: ")), 'second': int(input("Enter second year: "))}
            result_scan_movies=scan_movies(table_name, movie_year_range )
            print("=*="*10)
            print ("Movies released in %s" % movie_year_range)
            print ("=*="*10)
            for i in result_scan_movies:
                print (i['title'])
            print ("=*="*10)
        elif input_choice == "7":
            print("Thank you!!!")
            os.system('clear')
            sys.exit()
        elif input_choice > "8" or input_choice < "1":
            os.system('clear')
            print("=*="*10)
            print ("Select the choices between 1 to 7")
            print ("=*="*10)
    else:
        os.system('clear')
        print("=*="*10)
        print ("Invalid input choice")
        print ("=*="*10)