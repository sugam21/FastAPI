import time
from datetime import datetime

import numpy as np
from fastapi.responses import JSONResponse
from loguru import logger

from .save_to_mongodb import Mongo


def check_if_diabetic(prediction) -> None | str:
    """Condition to check if the result is Diabetic or not."""
    try:
        prediction_int = np.rint(prediction[0])
        if prediction_int == 1:
            return "Diabetic"
        else:
            return "Non-Diabetic"
    except Exception as e:
        logger.error(str(e))


def make_prediction(model, input_dict, model_name):
    """Predicts the outcome for provided data."""
    try:
        input = [list(input_dict.values())]
        start_time = time.time()
        prediction = model.predict(input)
        end_time = time.time()
    except Exception as e:
        logger.error(str(e))
        return JSONResponse(content={"message": str(e)}, status_code=500)
    else:
        is_diabetic: str = check_if_diabetic(prediction)
        response_content: dict = {
            "timestamp": datetime.now(),
            "input": input_dict,
            "output": is_diabetic,
            "model_used": model_name,
            "response_time": end_time - start_time,
        }
        # Inserting record into mongo db.
        db = Mongo()
        inserted_id = db.insert_record(
            collection_name="Diabetes", record=response_content
        )
        if inserted_id:
            response_content["_id"] = str(inserted_id)

        # Making timestamp json searealized
        response_content.update({"timestamp": str(response_content["timestamp"])})

        try:
            response = JSONResponse(
                content={"message": response_content},
                status_code=200,
            )
        except Exception as e:
            logger.error(str(e))
        else:
            return response
