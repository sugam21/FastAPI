from enum import Enum

from fastapi import FastAPI, HTTPException, Response
from loguru import logger
from pydantic import BaseModel, field_validator

from .load_model import load_model
from .predict import make_prediction

app = FastAPI()


class Diabetes(BaseModel):
    Pregnancies: int = 0
    PlasmaGlucose: int = 0
    DiastolicBloodPressure: int = 0
    TricepsThickness: int = 0
    SerumInsulin: int = 0
    BMI: float = 0.0
    DiabetesPedigree: float = 0.0
    Age: int = 0

    @field_validator(
        "Pregnancies",
        "PlasmaGlucose",
        "DiastolicBloodPressure",
        "TricepsThickness",
        "SerumInsulin",
        "BMI",
        "DiabetesPedigree",
        "Age",
        mode="after",
    )
    @classmethod
    def non_negative(cls, value: int | float, field):
        if value < 0:
            raise ValueError(f"{field.field_name} cannot be negative")
        return value

    @field_validator(
        "PlasmaGlucose",
        "DiastolicBloodPressure",
        "TricepsThickness",
        "SerumInsulin",
        "BMI",
        "DiabetesPedigree",
        "Age",
        mode="after",
    )
    @classmethod
    def non_zero(cls, value: int | float, field):
        if value == 0:
            raise ValueError(f"{field.field_name} must be greater than 0")
        return value


class ModelName(str, Enum):
    logisticRegression = "Logistic Regression"
    decisionTree = "Decision Tree"
    lgbm = "LGBM"


selected_model = None
selected_model_name = None


@app.get("/models/{model_name}")
async def choose_model(model_name: ModelName):
    """Lets user to choose model."""
    global selected_model
    global selected_model_name
    selected_model_name = model_name.value
    selected_model = load_model(model_name=model_name.value)
    logger.info(f"Model {model_name.value} selected.")
    return {"message": f"Model {model_name.value} selected."}


@app.post("/predict")
async def get_prediction(patient_info: Diabetes) -> Response:
    global selected_model
    global selected_model_name
    # Check if model is selected or not.
    if not selected_model:
        logger.error("No model selected. Please select a model first.")
        raise HTTPException(
            status_code=400, detail="No model selected. Please select a model first."
        )
    patient_info_dict = patient_info.model_dump()
    response = make_prediction(
        model=selected_model,
        input_dict=patient_info_dict,
        model_name=selected_model_name,
    )
    return response


if __name__ == "__main__":
    ...
