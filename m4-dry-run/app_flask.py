from flask import Flask, request
import joblib
import numpy as np

app = Flask(__name__)
model = joblib.load("model.joblib")


@app.route("/")
def index():
    return """
    <h1>Rain prediction API</h1>
    POST 25 climate model outputs to /predict as JSON: {"data": [...]}
    """


@app.route("/predict", methods=["POST"])
def predict():
    content = request.json
    features = np.array(content["data"]).reshape(1, -1)
    prediction = model.predict(features)[0]
    return {"prediction": float(prediction)}
