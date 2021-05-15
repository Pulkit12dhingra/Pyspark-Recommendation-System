# setting up the path to spark
import findspark
findspark.init('c:/spark')

# __pycache__ may cause reloading to the flask server so disabling the cache file
import sys
sys.dont_write_bytecode = True 

# get the pyspark libraries 
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

# other required library
import codecs
import os

# importing flask
from flask import Flask, render_template, request

# function to load the movie name
def loadMovieNames():
    movieNames = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open("data/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# function to reverse the name dictonary containg movieID as key and movie title as value 
# to movie title as key to movie ID as value.
# This is done to find the movie ID from the given input
def rev(d1):
    old_dict = d1
    new_dict = dict([(value, key) for key, value in old_dict.items()])
    return new_dict

# get the movie id from the given title
def getMovieID(name,movieNames):	

	# reverse the dictonary of movie names
	movies=rev(movieNames)
	
	# loop through each key(title)
	for i in movies:
	
		# convert to lower case and compare with the entered title
		if i.lower()[:len(i)-7]==name.lower():
	
			# retutn if found
			return movies[i]
		else:
			pass
	
	# if not found in the entire dataset then return 0
	else:
		return 0

# start sparks session
spark = SparkSession.builder.appName("ALSReccom").getOrCreate()

# initiate the flask app
app = Flask(__name__)

# load the model
model_path="M:/spark project/movie recommendation/als_model"
print(model_path)
model = ALSModel.load(model_path)

# defining the schema    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# getting the movie names    
names = loadMovieNames()
    
# getting the ratings 
ratings = spark.read.option("sep", "\t").schema(moviesSchema) \
    .csv("data/ml-100k/u.data")

# load the index page
@app.route("/")
def home():
    return render_template("index.html")

# communication with the frontend chatbot
@app.route("/get")
def get_bot_response():

	# get the entry from front end
	entry = request.args.get('msg')
	if entry=='quit':
		spark.stop()

	# get the movie ID of the entered movie
	userID=getMovieID(entry,names)

	# if returned 0 then movie not present
	if userID == 0:
		return "Sorry but I have no data about the movie please try another one :)"

	else: 
		# Manually create adataframe to get recommendations
		# get schema
		userSchema = StructType([StructField("userID", IntegerType(), True)])

		# create the dataframe
		users = spark.createDataFrame([[userID,]], userSchema)
		
		# use the loaded model to get predictions
		recommendations = model.recommendForUserSubset(users, 10).collect()

		# initialize parameters to save the results
		l=[]
		s=" "
		for userRecs in recommendations:

			# userRecs is (userID, [Row(movieId, rating), Row(movieID, rating)...])
		    myRecs = userRecs[1]  

		    # my Recs is just the column of recs for the user
		    for rec in myRecs: 

		        # For each rec in the list, extract the movie ID and rating
		        movie = rec[0] 
		        movieName = names[movie]
		     	
		     	# save the predicted movie in a list 
		        l.append(movieName)

		# return the string containing all the movies
		return s.join(l)

# flask main 
if __name__ == "__main__":
    app.run(debug=False)