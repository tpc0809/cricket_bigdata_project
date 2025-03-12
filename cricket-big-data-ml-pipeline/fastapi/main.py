from fastapi import FastAPI
from pydantic import BaseModel

# Initialize FastAPI app
app = FastAPI()

# Define a request model
class Item(BaseModel):
    name: str
    price: float
    description: str = None

# Root Endpoint
@app.get("/")
def read_root():
    return {"message": "FastAPI is running successfully!"}

# Sample Endpoint: Get Item by ID
@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "query": q}

# Sample Endpoint: Create Item
@app.post("/items/")
def create_item(item: Item):
    return {"message": "Item created successfully!", "item": item}