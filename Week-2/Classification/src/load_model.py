import pickle

from loguru import logger

from .path import path


def load_model(model_name: str = "LGBM"):
    """Loads the given model into memory."""
    match model_name:
        case "Logistic Regression":
            try:
                with open(
                    path.get("model_save_dir") / "logistic_regression.pkl", mode="rb"
                ) as file:
                    model = pickle.load(file)
            except Exception as e:
                logger.error(str(e))
                return None
            else:
                return model
        case "Decision Tree":
            try:
                with open(
                    path.get("model_save_dir") / "decision_tree.pkl", mode="rb"
                ) as file:
                    model = pickle.load(file)
            except Exception as e:
                logger.error(str(e))
                return None
            else:
                return model
        case "LGBM":
            try:
                with open(path.get("model_save_dir") / "lgb.pkl", mode="rb") as file:
                    model = pickle.load(file)
            except Exception as e:
                logger.error(str(e))
                return None
            else:
                return model
        case _:
            raise ValueError("Model Name not available")
