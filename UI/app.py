from flask import Flask, render_template, url_for, request
from markupsafe import escape

app = Flask(__name__)

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/data')
def data():
  return render_template('index.html')

if __name__ == "__main__":
  app.run(debug=True)