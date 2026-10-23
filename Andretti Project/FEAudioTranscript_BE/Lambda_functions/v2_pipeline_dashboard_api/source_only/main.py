# """FastAPI application entrypoint.

# Run locally:   uvicorn main:app --reload --port 8000
# Docs:          http://localhost:8000/docs
# """

# from __future__ import annotations

# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from mangum import Mangum

# from config import settings
# from routers import dashboard, driver, metrics, pipeline, race

# app = FastAPI(
#     title=settings.api_title,
#     version=settings.api_version,
#     root_path="/test",
# )

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=settings.cors_origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# app.include_router(dashboard.router)
# app.include_router(pipeline.router)
# app.include_router(race.router)
# app.include_router(driver.router)
# app.include_router(metrics.router)


# @app.get("/health", tags=["health"])
# def health():
#     return {
#         "status": "ok",
#         "service": settings.api_title,
#         "version": settings.api_version,
#     }


# @app.get("/", tags=["health"])
# def root():
#     return {"service": settings.api_title, "docs": "/docs"}


# from fastapi import Request


# @app.middleware("http")
# async def debug_request(request: Request, call_next):
#     print("DEBUG PATH:", request.url.path)
#     print("DEBUG HEADERS:", dict(request.headers))
#     response = await call_next(request)
#     print("DEBUG STATUS:", response.status_code)
#     return response


# handler = Mangum(app, lifespan="off")



############## 16/07/2026 #########
"""FastAPI application entrypoint.

Run locally:   uvicorn main:app --reload --port 8000
Docs:          http://localhost:8000/docs
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

from config import settings
from routers import dashboard, driver, metrics, pipeline, race

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    root_path="/test",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard.router)
app.include_router(pipeline.router)
app.include_router(race.router)
app.include_router(driver.router)
app.include_router(metrics.router)


@app.get("/health", tags=["health"])
def health():
    return {
        "status": "ok",
        "service": settings.api_title,
        "version": settings.api_version,
    }


@app.get("/", tags=["health"])
def root():
    return {"service": settings.api_title, "docs": "/docs"}


from fastapi import Request


@app.middleware("http")
async def debug_request(request: Request, call_next):
    print("DEBUG PATH:", request.url.path)
    print("DEBUG HEADERS:", dict(request.headers))
    response = await call_next(request)
    print("DEBUG STATUS:", response.status_code)
    return response


handler = Mangum(app, lifespan="off")



