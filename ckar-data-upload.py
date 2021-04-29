import boto3
import pymongo
from pymongo import MongoClient
import pandas as pd
import os
import re

s3_client = boto3.client('s3')
mongodb_pwd = os.getenv('MONGODB_PWD')


def put_data(df, collection):
    
    count_collection = collection.count()
    
    if(count_collection > 0):
        latest_date_collection = collection.find_one(sort=[("time", -1)])["time"]
        df = df[df['time'] > latest_date_collection]
        df_dict = df.to_dict("records")
        for doc in df_dict:
            collection.insert_one(doc)
    elif (count_collection == 0):
        df_dict = df.to_dict("records")
        for doc in df_dict:
            collection.insert_one(doc)

def lambda_handler(event, context):
    try:
        # S3 bucket name that contains the csv files
        bucket_name = 'ckar-test'

        # English survey responses file name
        s3_file_name_eng = 'english.csv'
        resp_eng = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name_eng)

        # English survey dataframe
        df_eng = pd.read_csv(resp_eng['Body'], sep=',', dtype={2: 'object'})
        df_eng = df_eng.fillna('')
        df_eng.columns = ['time', 'visit_reason', 'zipcode', 'age', 'gender',
                          'helpful_programs', 'community_challenges', 'information_sharing_media', 'email',
                          'links_to', 'other_comments']
        df_eng['information_sharing_store'] = ''
        df_eng['time'] = pd.to_datetime(df_eng['time'])
        for col in ['visit_reason', 'helpful_programs', 'community_challenges', 'information_sharing_media', 'links_to']:
            df_eng[col] = df_eng[col].apply(lambda x: re.split(",(?![^()]*\\))\\s*", x))
        df_eng = df_eng.sort_values(by=['time'])

        # Spanish survey responses file name
        s3_file_name_esp = 'spanish.csv'
        resp_esp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name_esp)

        # Spanish survey dataframe
        df_esp = pd.read_csv(resp_esp['Body'], sep=',', dtype={2: 'object'})
        df_esp = df_esp.fillna('')
        df_esp.columns = ['time', 'visit_reason', 'zipcode', 'age', 'gender',
                          'helpful_programs', 'community_challenges', 'information_sharing_media',
                          'information_sharing_store', 'links_to',
                          'email', 'other_comments']
        df_esp['time'] = pd.to_datetime(df_esp['time'])
        for col in ['visit_reason', 'helpful_programs', 'community_challenges', 'information_sharing_media', 'links_to']:
            df_esp[col] = df_esp[col].apply(lambda x: re.split(",(?![^()]*\\))\\s*", x))
        df_esp = df_esp.sort_values(by=['time'])

        client = MongoClient(
            f'mongodb+srv://test-user:{mongodb_pwd}@ckar-data.qaeqp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
        db = client["ckar_data"]

        eng_collection = db['eng_data']
        esp_collection = db['esp_data']

        put_data(df_eng, eng_collection)
        put_data(df_esp, esp_collection)

        client.close()
        print("Export successful")

    except Exception as err:
        print(err)

    return "Finished execution"
