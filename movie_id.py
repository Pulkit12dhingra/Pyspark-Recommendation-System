import findspark
findspark.init('c:/spark')
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def getMovieID(name,movieNames):
  df=movieNames.select("*").toPandas()
  movies=df["movieTitle"]
  for movie in range(len(movies)):
    
    if name.lower() in movies[movie].lower():
      return df['movieID'][movie]
    else:
      pass
  return 0    


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("ml-100k/u.item")
name=input("Enter the movie of your choice :- ")
idx=getMovieID(name,movieNames)
print(type(idx))