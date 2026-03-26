from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import data_files, logs
from .utils.logger import get_logger, log_execution


logger = get_logger(__name__)

app = FastAPI(
    title = "JBC API",
    description = "API for Sales Data"
    )

# ensure browser can access our api
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # would be replaced with web url if hosted publicly 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

@log_execution
@app.get("/")
def get_root():
    return {"message": "Hello from main"}

# add router endpoints to app!
app.include_router(data_files.convertRouter)
app.include_router(data_files.queryRouter)
app.include_router(logs.router)


def start_server():
    """
    Launch the API server with Uvicorn
    """
    import uvicorn
    uvicorn.run("app.server:app", host="localhost", port=8000, reload=True)