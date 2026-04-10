"""DataPulse v5 — FastAPI Ana Uygulama (backend/app.py)

PYTHONPATH=/app zorunlu → "from backend.xxx" import'ları çalışır.
Railway: railway.toml builder="DOCKERFILE" → Dockerfile CMD ile başlatılır.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List

from fastapi import FastAPI, Form, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from backend.core.config import settings
from backend.routes import dashboard, payment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


# ── WebSocket Bağlantı Yöneticisi ────────────────────────────
class ConnectionManager:
    def __init__(self) -> None:
        self._connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, job_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.setdefault(job_id, []).append(ws)

    def disconnect(self, job_id: str, ws: WebSocket) -> None:
        sockets = self._connections.get(job_id, [])
        if ws in sockets:
            sockets.remove(ws)
        if not sockets:
            self._connections.pop(job_id, None)

    async def broadcast(self, job_id: str, message: dict) -> None:
        dead: List[WebSocket] = []
        for ws in self._connections.get(job_id, []):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(job_id, ws)


ws_manager = ConnectionManager()


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"DataPulse v5 başlatılıyor... env={settings.ENVIRONMENT}")
    yield
    logger.info("DataPulse v5 kapatılıyor...")


# ── FastAPI App ───────────────────────────────────────────────
app = FastAPI(
    title="DataPulse v5",
    version="5.0.0",
    description="Production-grade veri otomasyon platformu",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routers
app.include_router(dashboard.router, prefix="/api/v1", tags=["dashboard"])
app.include_router(payment.router, prefix="/api/v1/payment", tags=["payment"])


# ── Auth endpoint (frontend için) ────────────────────────────
@app.post("/api/v1/auth/login", tags=["auth"])
async def login(email: str = Form(), password: str = Form()) -> dict:
    """Supabase email/password login → JWT token döndür."""
    from backend.services.supabase_service import SupabaseService
    svc = SupabaseService()
    if not svc._client:
        # Demo mod
        return {"token": "demo-token", "email": email, "plan": "pro"}
    try:
        result = svc._client.auth.sign_in_with_password(
            {"email": email, "password": password}
        )
        return {
            "token": result.session.access_token,
            "email": result.user.email,
            "plan": "free",
        }
    except Exception as e:
        raise JSONResponse(status_code=401, content={"detail": str(e)})


@app.post("/api/v1/auth/register", tags=["auth"])
async def register(email: str = Form(), password: str = Form()) -> dict:
    """Yeni kullanıcı kaydı."""
    from backend.services.supabase_service import SupabaseService
    svc = SupabaseService()
    if not svc._client:
        return {"token": "demo-token", "email": email}
    try:
        result = svc._client.auth.sign_up({"email": email, "password": password})
        token = result.session.access_token if result.session else ""
        return {"token": token, "email": result.user.email}
    except Exception as e:
        raise JSONResponse(status_code=400, content={"detail": str(e)})


# ── WebSocket ─────────────────────────────────────────────────
@app.websocket("/ws/jobs/{job_id}")
async def job_websocket(websocket: WebSocket, job_id: str) -> None:
    await ws_manager.connect(job_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(job_id, websocket)


# ── System endpoints ─────────────────────────────────────────
@app.get("/health", tags=["system"])
async def health() -> dict:
    return {"status": "ok", "version": "5.0.0", "env": settings.ENVIRONMENT}


# ── Frontend (SPA) ────────────────────────────────────────────
# Not: assets/ boş olduğu için StaticFiles mount yok — her şey index.html içinde inline

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
@app.get("/login", response_class=HTMLResponse, include_in_schema=False)
async def serve_frontend() -> HTMLResponse:
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return HTMLResponse(index.read_text())
    return HTMLResponse("<h1>Frontend bulunamadı</h1>")


@app.exception_handler(Exception)
async def global_exception_handler(request, exc: Exception) -> JSONResponse:
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "detail": str(exc)},
    )


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ── WebSocket Bağlantı Yöneticisi ────────────────────────────
class ConnectionManager:
    """Job bazlı WebSocket bağlantılarını yönetir."""

    def __init__(self) -> None:
        self._connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, job_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.setdefault(job_id, []).append(ws)
        logger.info(f"WS connected: job={job_id}")

    def disconnect(self, job_id: str, ws: WebSocket) -> None:
        sockets = self._connections.get(job_id, [])
        if ws in sockets:
            sockets.remove(ws)
        if not sockets:
            self._connections.pop(job_id, None)
        logger.info(f"WS disconnected: job={job_id}")

    async def broadcast(self, job_id: str, message: dict) -> None:
        """Job için tüm bağlı client'lara mesaj gönder."""
        dead: List[WebSocket] = []
        for ws in self._connections.get(job_id, []):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(job_id, ws)

    async def send_log(self, job_id: str, level: str, text: str) -> None:
        await self.broadcast(job_id, {"type": "log", "level": level, "text": text})

    async def send_progress(self, job_id: str, pct: int) -> None:
        await self.broadcast(job_id, {"type": "progress", "pct": pct})

    async def send_done(self, job_id: str, result: dict) -> None:
        await self.broadcast(job_id, {"type": "done", "result": result})


ws_manager = ConnectionManager()


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        f"DataPulse v5 başlatılıyor... "
        f"env={settings.ENVIRONMENT} "
        f"redis={settings.REDIS_URL[:20]}..."
    )
    yield
    logger.info("DataPulse v5 kapatılıyor...")


# ── FastAPI App ───────────────────────────────────────────────
app = FastAPI(
    title="DataPulse v5",
    version="5.0.0",
    description="Production-grade veri otomasyon platformu",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(dashboard.router, prefix="/api/v1", tags=["dashboard"])
app.include_router(payment.router, prefix="/api/v1/payment", tags=["payment"])


# ── WebSocket Endpoint ────────────────────────────────────────
@app.websocket("/ws/jobs/{job_id}")
async def job_websocket(websocket: WebSocket, job_id: str) -> None:
    """Real-time job log + progress stream."""
    await ws_manager.connect(job_id, websocket)
    try:
        while True:
            # Client'tan gelen ping'leri al (bağlantıyı canlı tut)
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(job_id, websocket)


# ── Utility Endpoints ─────────────────────────────────────────
@app.get("/health", tags=["system"])
async def health() -> dict:
    return {"status": "ok", "version": "5.0.0", "env": settings.ENVIRONMENT}


@app.get("/", tags=["system"])
async def root() -> dict:
    return {
        "service": "DataPulse v5",
        "docs": "/docs",
        "health": "/health",
        "websocket": "/ws/jobs/{job_id}",
    }


@app.exception_handler(Exception)
async def global_exception_handler(request, exc: Exception) -> JSONResponse:
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "detail": str(exc)},
    )
