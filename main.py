import datetime
import json
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
from cache_pandas import timed_lru_cache
from fastapi import FastAPI
from loguru import logger
from pydantic import BaseModel

app = FastAPI()

CLEAN_DATA_DIR: Path = Path(".").resolve() / "data" / "cleaned"


class Customer(BaseModel):
    name: str
    age: int
    city: str
    state: str
    pincode: float | int


class Claims(BaseModel):
    han: str
    bill_amount: float | int
    status: str
    account_id: str


class Policy(BaseModel):
    han: str
    policy_name: str


def process_data() -> pd.DataFrame:
    """Fetch the cleaned csv's and merge them.

    Returns:
        merged csv."""
    try:
        account_df: pd.DataFrame = pd.read_csv(CLEAN_DATA_DIR / "account_cleaned.csv")

        claims_df: pd.DataFrame = pd.read_csv(CLEAN_DATA_DIR / "claims_cleaned.csv")

        policy_df: pd.DataFrame = pd.read_csv(CLEAN_DATA_DIR / "policies_cleaned.csv")
    except FileNotFoundError:
        return {"error": "CSV's files not found."}
    else:
        logger.info("data imported successfully.")

    merged_acc_claim: pd.DataFrame = pd.merge(
        left=account_df, right=claims_df, how="left", on="AccountId"
    )

    merged_acc_claim_policy: pd.DataFrame = pd.merge(
        left=merged_acc_claim, right=policy_df, how="left", on="HAN"
    )
    # Where HAN was Not available Policy Name will be Not Available
    merged_acc_claim_policy["Policy Name"] = merged_acc_claim_policy[
        "Policy Name"
    ].fillna("Not Available")
    logger.info("Data merged successfully.")
    logger.info(
        f"Null values in the data \n {merged_acc_claim_policy.isna().sum(axis=0).to_dict()}"
    )

    return account_df, claims_df, policy_df, merged_acc_claim_policy


account_df, claims_df, policy_df, df = process_data()


@app.get("/account/{account_id}")
@timed_lru_cache(seconds=100, maxsize=None)
def get_customer_info(account_id: str) -> list[dict] | dict:
    """Take an account id and return json object of fetched results.
    Params:
        account_id (str): Account Id of a customer
    Returns:
        result (list[dict] | dict]): A List of Dictionaries of records
    """
    try:
        if "df" not in globals() or not isinstance(df, pd.DataFrame):
            return {"error": "DataFrame named df is absent."}

        columns: list[str] = df.columns.to_list()

        if "AccountId" not in columns:
            return {"error": "Invalid column name AccountId."}

        fetched_data: np.ndarray = df.to_numpy()[
            df["AccountId"].to_numpy() == account_id
        ]

        if fetched_data.size == 0:
            return {"error": "Account not found."}

        result: list[dict[str, any]] = []
        for idx in range(len(fetched_data)):
            temp_dict: dict[str, any] = dict(zip(columns, fetched_data[idx]))
            result.append(temp_dict)

        return result
    except Exception as e:
        return {"error": f"An unexpected error occured {str(e)}"}


@app.post("/account/")
async def add_new_customer(customer: Customer) -> dict:
    account_id: str = uuid4().hex
    customer_name = customer.name.capitalize()
    city = customer.city.capitalize()
    state = customer.state.capitalize()
    account_df_path: Path = CLEAN_DATA_DIR / "account_cleaned.csv"

    try:
        account_df = pd.read_csv(account_df_path)
    except FileNotFoundError:
        return {"error": f"The mentioned data file is absent from {account_df_path}."}
    else:
        logger.info("Accounts data file read successfully.")

    if customer_name.strip() == "" or city.strip() == "" or state.strip() == "":
        return {"error": "Fields can not be empty"}

    new_user_info_dict = {
        "AccountId": account_id,
        "Name": customer_name,
        "Age": customer.age,
        "City": city,
        "State": state,
        "Pincode": customer.pincode,
    }
    try:
        account_df.loc[len(account_df)] = new_user_info_dict
        account_df.to_csv(account_df_path, index=False)
    except PermissionError:
        return {
            "error": "The file is open in another program. Close the file and try again."
        }
    else:
        logger.info("Data inserted successfully into accounts table.")

    return {"status": "200", "message": "success"}


@app.post("/claims")
async def add_new_claims(claims: Claims):
    id: str = uuid4().hex
    created_date: str = str(datetime.datetime.now())
    case_number: str = (uuid4().hex[:10]).upper()

    han = claims.han
    bill_amount = claims.bill_amount
    status = claims.status
    account_id = claims.account_id

    if (policy_df.to_numpy()[policy_df["HAN"].to_numpy() == han]).size == 0:
        return {"error": "Invalid HAN number."}
    if (
        account_df.to_numpy()[account_df["AccountId"].to_numpy() == account_id]
    ).size == 0:
        return {"error": "Account is not registered."}
    if status not in ["Paid", "Not Paid"]:
        return {"error": "Status value not valid. Options[Paid, Not Paid]"}
    if bill_amount < 0:
        return {"error": "Invalid bill amount. Bill amount cannot be less than 0."}

    new_claim_dict: dict[str, any] = {
        "Id": id,
        "CreatedDate": created_date,
        "CaseNumber": case_number,
        "HAN": han,
        "BillAmount": bill_amount,
        "Status": status,
        "AccountId": account_id,
    }
    logger.debug(new_claim_dict)

    try:
        claims_df["CreatedDate"] = pd.to_datetime(claims_df["CreatedDate"])
        claims_df.loc[len(claims_df)] = new_claim_dict
        claims_df.to_csv(CLEAN_DATA_DIR / "claims_cleaned.csv", index=False)
    except PermissionError:
        return {
            "error": "The file is open in another program. Close the file and try again."
        }
    else:
        logger.info("Data inserted successfully into claims table.")

    return {"status": "200", "message": "success"}


# TODO read dataframe into the function itself and put everything into a single excel.
@app.post("/policy")
async def add_new_policy(policy: Policy):

    try:
        policy_df = pd.read_csv(CLEAN_DATA_DIR / "policies_cleaned.csv")
    except FileNotFoundError:
        return {"error": "The Policy data file is absent or spelling mismatch."}
    else:
        logger.info("Policy data file read successfully.")

    if (policy_df.to_numpy()[policy_df["HAN"].to_numpy() == policy.han]).size > 0:
        return {"error": "HAN number already exists."}
    if (
        policy_df.to_numpy()[policy_df["Policy Name"].to_numpy() == policy.policy_name]
    ).size > 0:
        return {"error": "Policy Name already exists."}

    if policy.han.strip(" ") == "" or policy.policy_name.strip(" ") == "":
        return {"error": "Empty HAN or Policy Name"}

    new_policy_dict: dict[str, any] = {
        "HAN": policy.han,
        "Policy Name": policy.policy_name,
    }

    try:
        policy_df.loc[len(policy_df)] = new_policy_dict
        policy_df.to_csv(CLEAN_DATA_DIR / "policies_cleaned.csv", index=False)
    except PermissionError:
        return {
            "error": "The file is open in another program. Close the file and try again."
        }
    else:
        logger.info("Data inserted successfully into policy table.")

    return {"status": "200", "message": "success"}


@app.delete("/account/{account_id}")
def delete_accounts(account_id: str):

    if not any(account_df["AccountId"].to_numpy() == account_id):
        return {"error": "Account not found."}
    try:
        idx = (account_df["AccountId"].to_numpy() == account_id).argmax()
        account_df.drop(idx, inplace=True)
        account_df.to_csv(CLEAN_DATA_DIR / "accounts_cleaned.csv", index=False)
    except PermissionError:
        return {
            "error": "The file is open in another program. Close the file and try again."
        }
    else:
        logger.info("Data deleted successfully from accounts table.")

    return {"status": "200", "message": "success"}


if __name__ == "__main__":
    print(get_customer_info(account_id="0018p000006TVysAAG"))
    # print(globals())
