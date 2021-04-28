from flask import Flask, render_template, request
import utils 

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")

@app.route("/get")
def get_bot_response():
	userText = request.args.get('msg')
	
	l=utils.session(userText)

	return l


if __name__ == "__main__":
    app.run(debug=True)