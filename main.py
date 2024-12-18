import datetime
from uuid import uuid4

import indiapins
import numpy as np
import pandas as pd
from cache_pandas import timed_lru_cache
from fastapi import FastAPI
from loguru import logger
from pydantic import BaseModel

from data import get_data, get_merged_data, save_data

app = FastAPI()


class Customer(BaseModel):
    Name: str | None = None
    Age: int | None = None
    City: str | None = None
    State: str | None = None
    Pincode: float | int | None = None


class Claims(BaseModel):
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


@app.post("/claims")
async def add_new_claims(claim: Claims):
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
        "Policy Name": policy.PoliyName,
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


@app.put("/account/{account_id}")
async def update_account(account_id: str, customer: Customer):
    accounts_df: pd.DataFrame = get_data(sheet_name="Accounts")
    if not any(accounts_df["AccountId"].to_numpy() == account_id):
        return {"error": "Account Not Found."}

    try:
        idx = (accounts_df["AccountId"].to_numpy() == account_id).argmax()
        stored_item_data = (accounts_df.iloc[idx, :]).to_dict()
        logger.debug(stored_item_data)
        stored_item_model = Customer(**stored_item_data)
        updated_data = stored_item_model.model_dump(
            exclude_unset=True, exclude_defaults=True
        )
        logger.debug(updated_data)
        updated_item = stored_item_model.model_copy(updated_data)
        logger.debug(updated_item)
        accounts_df.iloc[idx, :] = updated_item
        return updated_item

    except Exception as e:
        logger.debug(e)
