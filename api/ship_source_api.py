import os
import sys
import requests
from io import StringIO
from datetime import datetime
from configparser import ConfigParser
import pandas as pd
from pandas import json_normalize
import urllib, json
import pyspark.sql
from pyspark.sql import SparkSession as SS
import pyspark.sql.functions as f
import re
from pyspark.sql.functions import col
import flatten_json
from flatten_json import flatten

#def create_spark_session():
#    spark = SS.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
#    return spark


def api_request():
    try:
        #separate token from url
        url = ""
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as http_err:
        print('HTTP error occurred: {}'.format(http_err))
        print('System abort!')
        sys.exit()
    except Exception as e:
        print('Failed to request data from API.')
        print('System abort!')
        print(str(e))
        sys.exit()
    else:
        print('API request success!')
        return response


def api_data(ResponseData):
    df = ResponseData.json()
    data = json_normalize(flatten(df))
    base = data[[
            'cod',
            'message',
            'cnt',
            'list_0_dt'
            ]]
    print(base)
    #df2 = df[:'temp']
    #df = df.to_json(r"~/git/big-data-etl/file.json")
    #df_loc = "../file.json"
    #df2 = spark.read.json(df_loc, multiLine=True)
    #nested_df = json_normalize(df2)
    
    #need to find a way to explode list better
    #grab specific columns
    #df3 = df2[['list.0.clouds']]
    
    #.show()
        


if __name__ == "__main__":
    #spark = create_spark_session() 
    response =  api_request()
    api_data(ResponseData=response)
    

    

