from fastapi import APIRouter

from .debug_api import router as debug_router
from .file_chunk_api import router as file_chunk_router
from .internal_api import router as internal_router

router = APIRouter()
router.include_router(file_chunk_router)
router.include_router(internal_router)
router.include_router(debug_router)

