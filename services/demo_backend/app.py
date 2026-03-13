from fastapi import FastAPI

from api.demo_api import router as demo_router


app = FastAPI(title="317_DFS_Demo_Backend", version="0.2-p00")
app.include_router(demo_router)
