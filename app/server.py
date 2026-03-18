from fastapi import FastAPI

app = FastAPI(
    title = "JBC API",
    description = "API for Sales Data"
    )


# when we define routers in routers folder
# app.include_router(salesroutes.router)

@app.get("/")
def get_root():
    return {"message": "Hello from main"}


def start_server():
    """
    Launch the API server with Uvicorn
    """
    import uvicorn
    uvicorn.run("server:app", host="localhost", port=8000, reload=True)