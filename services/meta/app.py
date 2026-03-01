from fastapi import FastAPI
from routers.client_api import router as client_router

# main app for init router, logic is in routers and core
# 0.1p03 - storage heartbeat refresh + membership
app = FastAPI(title="317_DFS_Meta", version="0.1-p3")
app.include_router(client_router)