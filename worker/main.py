import os
import socket
import httpx
import asyncio
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

MASTER_URL = os.getenv("MASTER_URL", "http://master:8000")
STORAGE_DIR = "/app/storage"

WORKER_HOSTNAME = socket.gethostname() # Obtenemos el ID exacto del contenedor (ej. "f3b2a1")

os.makedirs(STORAGE_DIR, exist_ok=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Al arrancar, se registra en el Master con su Hostname exacto
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{MASTER_URL}/register_worker?worker_hostname={WORKER_HOSTNAME}")
            print(f"Worker {WORKER_HOSTNAME} registrado en el Master.")
        except Exception as e:
            print(f"Error registrando worker: {e}")
    yield

app = FastAPI(title=f"DataNode - {WORKER_HOSTNAME}", lifespan=lifespan)

@app.post("/store_block/{block_id}")
async def store_block(block_id: str, request: Request):
    """Recibe un bloque ENTERO y lo guarda en disco."""
    file_path = os.path.join(STORAGE_DIR, f"{block_id}_{WORKER_HOSTNAME}.dat")
    
    with open(file_path, "wb") as f:
        async for chunk in request.stream():
            f.write(chunk)
            
    print(f"[{WORKER_HOSTNAME}] Bloque {block_id} guardado con éxito.")
    return {"status": "stored"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)