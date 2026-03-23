from fastapi import FastAPI
from .routers import data_files

app = FastAPI(
    title = "JBC API",
    description = "API for Sales Data"
    )


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