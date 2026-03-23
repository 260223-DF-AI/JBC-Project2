import logging
from fastapi import FastAPI

# Configure logging on startup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log")
    ]
)

logger = logging.getLogger(__name__)

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


# when we define routers in routers folder
# app.include_router(salesroutes.router)

@app.get("/")
def get_root():
    return {"message": "Hello from main"}

@app.post("/convert")
def post_convert():
    """
    Trigger conversion of .csv from local storage and process into .parquet
    cloud asset, then upload to GCS
    """


def start_server():
    """
    Launch the API server with Uvicorn
    """
    import uvicorn
    uvicorn.run("server:app", host="localhost", port=8000, reload=True)