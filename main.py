from typing import Union

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def get_root() -> dict[str, str]:
    return {"Hello": "World!!!"}


@app.get("/items/{item_id}")
async def read_item(item_id: int, q: str | None = None) -> dict[str, int | str]:
    return {"item_id": item_id, "q": q}
