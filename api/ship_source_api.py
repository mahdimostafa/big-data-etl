import os
import sys
import requests
from io import StringIO
from datetime import datetime
from configparser import ConfigParser
import pandas as pd
from pandas.io.json import json_normalize
import urllib, json
import pyspark.sql
from pyspark.sql import SparkSession as SS
import pyspark.sql.functions as f

def create_spark_session():
    spark = SS.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def api_request():
    pass
    #try:
        #separate token from url
       # url = ""
        #response = requests.get(url)
        #response.raise_for_status()
    #except HTTPError as http_err:
        #print('HTTP error occurred: {}'.format(http_err))
        #print('System abort!')
        #sys.exit()
    #except Exception as e:
        #print('Failed to request data from API.')
        #print('System abort!')
        #print(str(e))
        #sys.exit()
    #else:
        #print('API request success!')
        #return response

def api_data(response, spark):
    #df = json_normalize(response.json())
    #df2 = df[:'temp']
    #df = df.to_json(r"~/git/big-data-etl/file.json")
    df_loc = "../file.json"
    df2 = spark.read.json(df_loc, multiLine=True)
    #need to find a way to explode list better
    #flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    #nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    #flat_df = nested_df.select(flat_cols +
    #                           [F.col(nc+'.'+c).alias(nc+'_'+c)
    #                            for nc in nested_cols
    #                            for c in nested_df.select(nc+'.*').columns])
    #return flat_df
    nested_df = df2.select(f.col("list.*"))
    flat_cols = [array[0] for array in df2.dtypes if array[1][:6] != 'struct']
    nested_cols = [array[0] for array in df2.dtypes if array[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    flat_df.printSchema()
    #df2.show()

    #grab specific columns
    #df3 = df2[['city.timezone']]
    
    #df.printSchema()
        


if __name__ == "__main__":
    spark = create_spark_session()
    response =  api_request()
    api_data(response, spark)
    

    

