import datetime
from uuid import uuid4

import numpy as np
import pandas as pd
from cache_pandas import timed_lru_cache
from fastapi import FastAPI
from loguru import logger
from pydantic import BaseModel

from .data import get_data, get_merged_data, save_data

app = FastAPI()


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
    customer_name = customer.name.capitalize()
    city = customer.city.capitalize()
    state = customer.state.capitalize()

    if customer.age <= 0:
        return {"error": "Invalid Age. Cannot be less than or equal to 0."}

    accounts_df: pd.DataFrame = get_data(sheet_name="Accounts")

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
    accounts_df.loc[len(accounts_df)] = new_user_info_dict
    result = save_data(accounts_df, sheet_name="Accounts")
    return result


@app.post("/claims")
async def add_new_claims(claim: Claims):
    id: str = uuid4().hex
    created_date: str = str(datetime.datetime.now())
    case_number: str = (uuid4().hex[:10]).upper()

    policies_df: pd.DataFrame = get_data(sheet_name="Policies")
    accounts_df: pd.DataFrame = get_data(sheet_name="Accounts")
    claims_df: pd.DataFrame = get_data(sheet_name="Claims")

    if (policies_df.to_numpy()[policies_df["HAN"].to_numpy() == claim.han]).size == 0:
        return {"error": "Invalid HAN number."}
    if (
        accounts_df.to_numpy()[accounts_df["AccountId"].to_numpy() == claim.account_id]
    ).size == 0:
        return {"error": "Account is not registered."}
    if claim.status not in ["Paid", "Not Paid"]:
        return {"error": "Status value not valid. Options[Paid, Not Paid]"}
    if claim.bill_amount < 0:
        return {"error": "Invalid bill amount. Bill amount cannot be less than 0."}

    new_claim_dict: dict[str, any] = {
        "Id": id,
        "CreatedDate": created_date,
        "CaseNumber": case_number,
        "HAN": claim.han,
        "BillAmount": claim.bill_amount,
        "Status": claim.status,
        "AccountId": claim.account_id,
    }
    logger.debug(new_claim_dict)

    claims_df.loc[len(claims_df)] = new_claim_dict
    result = save_data(claims_df, sheet_name="Claims")
    return result


@app.post("/policy")
async def add_new_policy(policy: Policy):

    policies_df: pd.DataFrame = get_data(sheet_name="Policies")

    if (policies_df.to_numpy()[policies_df["HAN"].to_numpy() == policy.han]).size > 0:
        return {"error": "HAN number already exists."}
    if (
        policies_df.to_numpy()[
            policies_df["Policy Name"].to_numpy() == policy.policy_name
        ]
    ).size > 0:
        return {"error": "Policy Name already exists."}

    if policy.han.strip(" ") == "" or policy.policy_name.strip(" ") == "":
        return {"error": "Empty HAN or Policy Name"}

    new_policy_dict: dict[str, any] = {
        "HAN": policy.han,
        "Policy Name": policy.policy_name,
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


@app.delete("/claims/{claim_id}")
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
