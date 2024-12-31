from fastapi import FastAPI
from core.config import settings
from api.v1.router import api_router
# import uvicorn

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/")
def root():
    return {"message": "Welcome to Product Catalog API"}


# if __name__ == "__main__":
#     uvicorn.run("main:app", host="127.0.0.1", port=5000, log_level="info")