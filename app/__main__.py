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