from fastapi import FastAPI
from routers.client_api import router as client_router

app = FastAPI(title="317_DFS_Meta", version="0.1-p2")
app.include_router(client_router)