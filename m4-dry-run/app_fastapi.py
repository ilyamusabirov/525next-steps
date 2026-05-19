from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np

app = FastAPI()

# Load once at startup, not on every request
model = joblib.load("model.joblib")


class PredictRequest(BaseModel):
    data: list[float]


class PredictResponse(BaseModel):
    prediction: float


@app.get("/")
def index():
    return {"message": "Rain prediction API. POST 25 climate model outputs to /predict."}


@app.post("/predict", response_model=PredictResponse)
def predict(body: PredictRequest):
    features = np.array(body.data).reshape(1, -1)
    prediction = model.predict(features)[0]
    return PredictResponse(prediction=float(prediction))
