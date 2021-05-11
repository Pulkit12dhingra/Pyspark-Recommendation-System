# setting up path to spark
import findspark
findspark.init('c:/spark')

# __pycache__ may cause reloading to the flask server so disabling the cache file
import sys
sys.dont_write_bytecode = True 

# importing pyspark libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS

# importing other requred libraries
import sys
import codecs
import os

# function to get the movie names
def loadMovieNames():
    movieNames = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open("data/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# start the spark session
spark = SparkSession.builder.appName("ALSReccom").getOrCreate()
  
# schema definition  
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
# get movie names    
names = loadMovieNames()

# get ratings based on schema
ratings = spark.read.option("sep", "\t").schema(moviesSchema) \
    .csv("data/ml-100k/u.data")

    
print("Training recommendation model...")

# create an ALS model
als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID") \
    .setRatingCol("rating")
 
# train the model   
model = als.fit(ratings)

# Get the current working directory
BASE_DIR = os.getcwd() 

# create a folder to store the model
model_path = BASE_DIR + "/als_model"

# store the model
model.write().overwrite().save(model_path)

print("Model is saved")