from fastapi import FastAPI

from .routers import data_files
from utils.logger import get_logger


logger = get_logger(__name__)

app = FastAPI(
    title = "JBC API",
    description = "API for Sales Data"
    )


@app.on_event("startup")
async def startup_event():
    """Initialize logging and app resources on startup"""
    logger.info("Starting JBC Sales Data API")
    logger.info("Logging system initialized")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    logger.info("Shutting down JBC Sales Data API")


@app.get("/")
def get_root():
    return {"message": "Hello from main"}

# add router endpoints to app!
app.include_router(data_files.router)


def start_server():
    """
    Launch the API server with Uvicorn
    """
    import uvicorn
    uvicorn.run("app.server:app", host="localhost", port=8000, reload=True)