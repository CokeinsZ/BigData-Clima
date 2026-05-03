import os
import socket
import httpx
import asyncio
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
from contextlib import asynccontextmanager

MASTER_URL = os.getenv("MASTER_URL", "http://master:8000")
WORKER_HOSTNAME = socket.gethostname() # Obtenemos el ID exacto del contenedor (ej. "f3b2a1")
STORAGE_DIR = f"/app/storage/{WORKER_HOSTNAME}" # Cada worker tiene su propio subdirectorio para evitar colisiones

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

@app.get("/read_block/{block_id}", response_class=PlainTextResponse)
async def read_block(block_id: str):
    """Devuelve el contenido crudo del bloque."""
    file_path = os.path.join(STORAGE_DIR, f"{block_id}_{WORKER_HOSTNAME}.dat")
    data = ""

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Bloque no encontrado en este nodo")
        
    with open(file_path, "r") as f:
        data = f.read()
    return data

@app.get("/process_block_average/{block_id}")
async def process_block_average(block_id: str, variable: str = "temperatura"):
    """Lee su archivo local, extrae la variable y devuelve la suma y el conteo."""
    file_path = os.path.join(STORAGE_DIR, f"{block_id}_{WORKER_HOSTNAME}.dat")
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Bloque no encontrado en este nodo")
        
    total_sum = 0.0
    count = 0
    
    with open(file_path, "r") as f:
        for line in f:
            if not line.strip(): 
                continue
            try:
                record = json.loads(line)
                # Navegamos por la estructura de tu JSON
                valor = record["sensores_climaticos"][0]["lecturas"][variable]["valor"]
                total_sum += valor
                count += 1
            except Exception as e:
                # Si hay una línea corrupta, la ignoramos y seguimos
                continue
                
    return {"sum": total_sum, "count": count, "worker_id": WORKER_HOSTNAME}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)