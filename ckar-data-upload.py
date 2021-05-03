import boto3
import pymongo
from pymongo import MongoClient
import pandas as pd
import os
import re
from google.oauth2 import service_account
import gspread

# S3 client
s3_client = boto3.client('s3')

def put_data(df, collection, lang):
    '''
    Inserts documents into the MongoDB database from a pandas dataframe.
    
    :param df: A pandas dataframe that contains the rows (individual survey responses) that need to be converted into documents for MongoDB.
    :param collection: The collection inside a mongodb database into which the documents will be inserted.
    :param lang: The language of the survey responses.
    
    :return: null
    '''
    #Get the number of documents of the given langauge (English or Spanish)
    count_collection_lang = collection.count_documents({"language":lang})
    
    # If the number of documents of the given language is greater than 0, get the latest date in the existing collection
    # and only insert data which is dated after the latest date in the collection
    if(count_collection_lang > 0):
        latest_date_collection = collection.find_one({"language": lang}, sort=[("time", -1)])["time"]
        df = df[df['time'] > latest_date_collection]
        df_dict = df.to_dict("records")
        for doc in df_dict:
            collection.insert_one(doc)
    # If there are no documents of the given language in the collection, simply insert them.
    elif (count_collection_lang == 0):
        df_dict = df.to_dict("records")
        for doc in df_dict:
            collection.insert_one(doc)

def lambda_handler(event, context):
    
    #Environment variables defined in AWS Lambda. Contains database credentials, Google Service Acount credentials, and the collection names.
    mongodb_pwd = os.getenv('MONGODB_PWD')
    bucket_name = os.getenv('BUCKET_NAME')
    key = os.getenv('KEY')
    eng_name = os.getenv('ENG_NAME')
    esp_name = os.getenv('ESP_NAME')
    
    try:
        # Download the Google Service account credentials to the /tmp folder of this funcgtion
        local_file_name = '/tmp/'+ key
        s3_client.download_file(bucket_name, key, local_file_name)
        scopes = ['https://www.googleapis.com/auth/drive',
                'https://spreadsheets.google.com/feeds']
        credentials = service_account.Credentials.from_service_account_file(
                local_file_name, scopes = scopes)
        
        # Authorize using the credentials
        gc = gspread.authorize(credentials)
        
        # English survey dataframe
        wks_eng = gc.open(eng_name).sheet1
        data_eng = wks_eng.get_all_values()
        headers_eng = data_eng.pop(0)
        df_eng = pd.DataFrame(data_eng, columns=headers_eng)

        #Data Wrangling
        df_eng = df_eng.fillna('')
        df_eng.columns = ['time', 'visit_reason', 'zipcode', 'age', 'gender',
                          'helpful_programs', 'community_challenges', 'information_sharing_media', 'email',
                          'links_to', 'other_comments']
        df_eng['information_sharing_store'] = ''
        df_eng['time'] = pd.to_datetime(df_eng['time'])
        df_eng['language'] = 'ENG'
        for col in ['visit_reason', 'helpful_programs', 'community_challenges', 'information_sharing_media', 'links_to']:
            df_eng[col] = df_eng[col].apply(lambda x: re.split(",(?![^()]*\\))\\s*", x))
        df_eng = df_eng.sort_values(by=['time'])

        # Spanish survey dataframe
        wks_esp = gc.open(esp_name).sheet1
        data_esp = wks_esp.get_all_values()
        headers_esp = data_esp.pop(0)
        df_esp = pd.DataFrame(data_esp, columns=headers_esp)

        #Data wrangling
        df_esp = df_esp.fillna('')
        df_esp.columns = ['time', 'visit_reason', 'zipcode', 'age', 'gender',
                          'helpful_programs', 'community_challenges', 'information_sharing_media',
                          'information_sharing_store', 'links_to',
                          'email', 'other_comments']
        df_esp['time'] = pd.to_datetime(df_esp['time'])
        df_esp['language'] = 'ESP'
        for col in ['visit_reason', 'helpful_programs', 'community_challenges', 'information_sharing_media', 'links_to']:
            df_esp[col] = df_esp[col].apply(lambda x: re.split(",(?![^()]*\\))\\s*", x))
        df_esp = df_esp.sort_values(by=['time'])
        
        # Set up the connection to the MongoDB database
        client = MongoClient(
            f'mongodb+srv://ckar-admin:{mongodb_pwd}@ckar-database-cluster.meitk.mongodb.net/test')
       
        db = client["ckar-surveys"]
        survey_collection = db['survey_data']

        put_data(df_eng, survey_collection, "ENG")
        put_data(df_esp, survey_collection, "ESP")

        client.close()
        os.remove(local_file_name)
        
        print("Export successful")

    except Exception as err:
        print("Export failed")
        print(err)

    return "Finished execution"
