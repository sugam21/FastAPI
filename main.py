import datetime
from uuid import uuid4

import indiapins
import numpy as np
import pandas as pd
from cache_pandas import timed_lru_cache
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel

from data import get_data, get_merged_data, save_data
from log import Log

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_method=["*"],
    allow_headers=["*"],
)


class Customer(BaseModel):
    Name: str | None = None
    Age: int | None = None
    City: str | None = None
    State: str | None = None
    Pincode: float | int | None = None


class Claim(BaseModel):
    HAN: str | None = None
    BillAmount: float | int | None = None
    Status: str | None = None
    AccountId: str | None = None


class Policy(BaseModel):
    HAN: str | None = None
    PolicyName: str | None = None


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
        merged_df: pd.DataFrame = get_merged_data()
        columns: list[str] = merged_df.columns.to_list()

        if "AccountId" not in columns:
            return {"error": "Invalid column name AccountId."}

        fetched_data: np.ndarray = merged_df.to_numpy()[
            merged_df["AccountId"].to_numpy() == account_id
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
    customer_name = customer.Name.capitalize()
    city = customer.City.capitalize()
    state = customer.State.capitalize()

    if customer.Age <= 0:
        return {"error": "Invalid Age. Cannot be less than or equal to 0."}

    try:
        indiapins.isvalid(str(customer.Pincode))
    except ValueError:
        return {"error": "Invalid Pincode. Must be in the format ######"}

    accounts_df: pd.DataFrame = get_data(sheet_name="Accounts")

    if customer_name.strip() == "" or city.strip() == "" or state.strip() == "":
        return {"error": "Fields can not be empty"}

    new_user_info_dict = {
        "AccountId": account_id,
        "Name": customer_name,
        "Age": customer.Age,
        "City": city,
        "State": state,
        "Pincode": customer.Pincode,
    }
    accounts_df.loc[len(accounts_df)] = new_user_info_dict
    result = save_data(accounts_df, sheet_name="Accounts")
    return result


@app.post("/claim")
async def add_new_claims(claim: Claim):
    id: str = uuid4().hex
    created_date: str = str(datetime.datetime.now())
    case_number: str = (uuid4().hex[:10]).upper()

    policies_df: pd.DataFrame = get_data(sheet_name="Policies")
    accounts_df: pd.DataFrame = get_data(sheet_name="Accounts")
    claims_df: pd.DataFrame = get_data(sheet_name="Claims")

    if (policies_df.to_numpy()[policies_df["HAN"].to_numpy() == claim.HAN]).size == 0:
        return {"error": "Invalid HAN number."}
    if (
        accounts_df.to_numpy()[accounts_df["AccountId"].to_numpy() == claim.AccountId]
    ).size == 0:
        return {"error": "Account is not registered."}
    if claim.Status not in ["Paid", "Not Paid"]:
        return {"error": "Status value not valid. Options[Paid, Not Paid]"}
    if claim.BillAmount < 0:
        return {"error": "Invalid bill amount. Bill amount cannot be less than 0."}

    new_claim_dict: dict[str, any] = {
        "Id": id,
        "CreatedDate": created_date,
        "CaseNumber": case_number,
        "HAN": claim.HAN,
        "BillAmount": claim.BillAmount,
        "Status": claim.Status,
        "AccountId": claim.AccountId,
    }
    logger.debug(new_claim_dict)

    claims_df.loc[len(claims_df)] = new_claim_dict
    result = save_data(claims_df, sheet_name="Claims")
    return result


@app.post("/policy")
async def add_new_policy(policy: Policy):

    policies_df: pd.DataFrame = get_data(sheet_name="Policies")

    if (policies_df.to_numpy()[policies_df["HAN"].to_numpy() == policy.HAN]).size > 0:
        return {"error": "HAN number already exists."}
    if (
        policies_df.to_numpy()[
            policies_df["Policy Name"].to_numpy() == policy.PolicyName
        ]
    ).size > 0:
        return {"error": "Policy Name already exists."}

    if policy.HAN.strip(" ") == "" or policy.PolicyName.strip(" ") == "":
        return {"error": "Empty HAN or Policy Name"}

    new_policy_dict: dict[str, any] = {
        "HAN": policy.HAN,
        "Policy Name": policy.PolicyName,
    }

    policies_df.loc[len(policies_df)] = new_policy_dict
    result = save_data(policies_df, sheet_name="Policies")
    return result


@app.delete("/account/{account_id}")
def delete_accounts(account_id: str):
    accounts_df: pd.DataFrame = get_data(sheet_name="Accounts")

    if not any(accounts_df["AccountId"].to_numpy() == account_id):
        return {"error": "Account not found."}

    idx = (accounts_df["AccountId"].to_numpy() == account_id).argmax()
    accounts_df.drop(idx, inplace=True)
    result = save_data(accounts_df, sheet_name="Accounts")
    return result


@app.delete("/claim/{claim_id}")
def delete_claims(claim_id: str):
    claims_df: pd.DataFrame = get_data(sheet_name="Claims")

    if not any(claims_df["Id"].to_numpy() == claim_id):
        return {"error": "Claim not found."}

    idx = (claims_df["Id"].to_numpy() == claim_id).argmax()
    claims_df.drop(idx, inplace=True)
    result = save_data(claims_df, sheet_name="Claims")
    return result


@app.delete("/policy/{han_number}")
def delete_poicy(han_number: str):
    policies_df: pd.DataFrame = get_data(sheet_name="Policies")

    if not any(policies_df["HAN"].to_numpy() == han_number):
        return {"error": "Policy not found."}

    idx = (policies_df["HAN"].to_numpy() == han_number).argmax()
    policies_df.drop(idx, inplace=True)
    result = save_data(policies_df, sheet_name="Policies")
    return result


@app.put("/account/{account_id}")
async def update_account(account_id: str, customer: Customer):
    accounts_df: pd.DataFrame = get_data(sheet_name="Accounts")
    if not any(accounts_df["AccountId"].to_numpy() == account_id):
        return {"error": "Account Not Found."}

    log_file = Log()

    try:
        idx = (accounts_df["AccountId"].to_numpy() == account_id).argmax()
        row_to_modify = accounts_df.iloc[idx, :]

        for col, new_value in customer.model_dump().items():
            if new_value != "string" and new_value != 0:
                old_val = row_to_modify[col]
                row_to_modify[col] = new_value
                accounts_df[idx] = row_to_modify
                result = save_data(accounts_df, sheet_name="Accounts")
                if result["status"] == 500:
                    return result

                log_file.write_log(
                    sheet_name="Accounts",
                    id=account_id,
                    column_name=col,
                    old_value=old_val,
                    new_value=new_value,
                )
    except Exception as e:
        logger.error(f"Error:  {e}")
    else:
        logger.info("Accounts sheet modified successfully")
        return {"status": 200, "message": "success"}


@app.put("/policy/{han_number}")
async def update_policy(han_number: str, policy: Policy):
    policies_df: pd.DataFrame = get_data(sheet_name="Policies")

    if not any(policies_df["HAN"].to_numpy() == han_number):
        return {"error": "Policy not found."}

    log_file = Log()
    flag: bool = True

    try:
        idx = (policies_df["HAN"].to_numpy() == han_number).argmax()
        row_to_modify = policies_df.iloc[idx, :]

        for col, new_value in policy.model_dump().items():
            if col == "PolicyName":
                col = "Policy Name"
            if new_value != "string" and new_value != 0:
                flag = False
                old_val = row_to_modify[col]
                row_to_modify[col] = new_value
                policies_df[idx] = row_to_modify
                result = save_data(policies_df, sheet_name="Policies")
                if result["status"] == 500:
                    return result

                log_file.write_log(
                    sheet_name="Policies",
                    id=han_number,
                    column_name=col,
                    old_value=old_val,
                    new_value=new_value,
                )
    except Exception as e:
        logger.error(f"Error:  {e}")
    else:
        if flag:
            return {"message": "Nothing to modify"}
        else:
            logger.info("Policies sheet modified successfully")
            return {"status": 200, "message": "success"}


@app.put("/claim/{claim_id}")
async def update_claim(claim_id: str, claim: Claim):
    claims_df: pd.DataFrame = get_data(sheet_name="Claims")

    if not any(claims_df["Id"].to_numpy() == claim_id):
        return {"error": "Claim not found."}

    log_file = Log()
    flag: bool = True

    try:
        idx = (claims_df["Id"].to_numpy() == claim_id).argmax()
        row_to_modify = claims_df.iloc[idx, :]

        for col, new_value in claim.model_dump().items():
            if new_value != "string" and new_value != 0:
                flag = False
                old_val = row_to_modify[col]
                row_to_modify[col] = new_value
                claims_df[idx] = row_to_modify
                result = save_data(claims_df, sheet_name="Claims")
                if result["status"] == 500:
                    return result

                log_file.write_log(
                    sheet_name="Claims",
                    id=claim_id,
                    column_name=col,
                    old_value=old_val,
                    new_value=new_value,
                )
    except Exception as e:
        logger.error(f"Error:  {e}")
    else:
        if flag:
            return {"message": "Nothing to modify"}
        else:
            logger.info("Claims sheet modified successfully")
            return {"status": 200, "message": "success"}
            return {"status": 200, "message": "success"}
