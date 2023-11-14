from fastapi import FastAPI
from routers import file

app = FastAPI()

app.include_router(file.router)
