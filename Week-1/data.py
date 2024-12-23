import pandas as pd
from loguru import logger

from path import CLEAN_DATA_PATH


def get_data(sheet_name: str) -> pd.DataFrame | dict:
    """Fetch the cleaned csv's and merge them.

    Returns:
        merged csv."""

    availabel_sheets: list[str] = ["Accounts", "Claims", "Policies"]
    match sheet_name:
        case "Accounts":
            try:
                account_df: pd.DataFrame = pd.read_excel(
                    CLEAN_DATA_PATH, sheet_name="Accounts"
                )
            except FileNotFoundError:
                return {"error": "Accounts sheet not found."}
            else:
                logger.info("Accounts sheet imported successfully.")
                return account_df
        case "Claims":
            try:
                claims_df: pd.DataFrame = pd.read_excel(
                    CLEAN_DATA_PATH, sheet_name="Claims"
                )
            except FileNotFoundError:
                return {"error": "Claims sheet not found."}
            else:
                logger.info("Claims sheet imported successfully.")
                return claims_df
        case "Policies":
            try:
                policies_df: pd.DataFrame = pd.read_excel(
                    CLEAN_DATA_PATH, sheet_name="Policies"
                )
            except FileNotFoundError:
                return {"error": "Policies sheet not found."}
            else:
                logger.info("Policies sheet imported successfully.")
                return policies_df
        case _:
            logger.error(
                f"Sheet name not in available sheets. Please provide one from: {availabel_sheets}"
            )
            return {
                "status": {
                    "code": 404,
                    "message": f"Sheet name not in available sheets. Please provide one from: {availabel_sheets}",
                }
            }


def save_data(data: pd.DataFrame, sheet_name: str) -> dict:
    """Saves the dataframe frame into given sheet.
    Params:
        data(pd.DataFrame): DataFrame to save.
        sheet_name(str): Sheet to save the data to.
    Returns:
        dict[str,str]: Success or error message.
    """

    availabel_sheets: list[str] = ["Accounts", "Claims", "Policies"]

    match sheet_name:
        case "Accounts":
            try:
                with pd.ExcelWriter(
                    CLEAN_DATA_PATH, mode="a", if_sheet_exists="replace"
                ) as writer:
                    data.to_excel(writer, sheet_name="Accounts", index=False)
            except PermissionError:
                return {
                    "status": 500,
                    "message": "The file is open in another program. Close the file and try again.",
                }
            else:
                logger.info("Data inserted successfully into Accounts sheet.")
                return {"status": 200, "message": "success"}
        case "Claims":
            try:
                with pd.ExcelWriter(
                    CLEAN_DATA_PATH, mode="a", if_sheet_exists="replace"
                ) as writer:
                    data.to_excel(writer, sheet_name="Claims", index=False)
            except PermissionError:
                return {
                    "status": 500,
                    "message": "The file is open in another program. Close the file and try again.",
                }
            else:
                logger.info("Data inserted successfully into Claims sheet.")
                return {"status": 200, "message": "success"}
        case "Policies":
            try:
                with pd.ExcelWriter(
                    CLEAN_DATA_PATH, mode="a", if_sheet_exists="replace"
                ) as writer:
                    data.to_excel(writer, sheet_name="Policies", index=False)
            except PermissionError:
                return {
                    "status": 500,
                    "message": "The file is open in another program. Close the file and try again.",
                }
            else:
                logger.info("Data inserted successfully into Policies sheet.")
                return {"status": 200, "message": "success"}
        case _:
            logger.error(
                f"Sheet name not in available sheets. Please provide one from: {availabel_sheets}"
            )
            return {
                "status": 500,
                "message": f"Sheet name not in available sheets. Please provide one from: {availabel_sheets}",
            }


def get_merged_data():
    policies_df: pd.DataFrame = get_data(sheet_name="Policies")
    accounts_df: pd.DataFrame = get_data(sheet_name="Accounts")
    claims_df: pd.DataFrame = get_data(sheet_name="Claims")
    merged_account_claim: pd.DataFrame = pd.merge(
        left=accounts_df, right=claims_df, how="left", on="AccountId"
    )
    merged_account_claim_policy: pd.DataFrame = pd.merge(
        left=merged_account_claim, right=policies_df, how="left", on="HAN"
    )
    # Where HAN was Not available Policy Name will be Not Available
    merged_account_claim_policy["Policy Name"] = merged_account_claim_policy[
        "Policy Name"
    ].fillna("Not Available")
    logger.info("Data merged successfully.")
    logger.debug(
        f"Null values in the data \n {merged_account_claim_policy.isna().sum(axis=0).to_dict()}"
    )
    return merged_account_claim_policy
