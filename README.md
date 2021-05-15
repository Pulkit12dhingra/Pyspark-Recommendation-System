# Pyspark-Recommendation-System

The project is a savior for binge-watchers spending lots of time finding their perfect fit in movies. The project deals with a movie recommendation system. It is powered by a spark dataframe which allows it to handle a large set of data ( which we call Big data) and make recommendations. 
The project runs on ALS recommendation model and runs over a web API. To make the project much more user interactive, the project uses a chatbot like interphase.

Here is what our interphase will look like:-

![Interphase_image](/git_img/img1.jpg)

## How to run??

There are few things you need to have as prerequisites before using the project. 
1. Have spark installed and running
2. Have Flask installed on your computer

### Procedure 
Having fulfilled all the requirements now is the easy part:-
1. Run the get_model.py file to train the model
2. After the model is created you'll have an "als_model" folder containing the model.
3. Now run app.py to start the flask app.
4. Go to localhost:5000 URL on your favorite browser, and you are good to go...
 
 :) Enjoy, Thanks.
