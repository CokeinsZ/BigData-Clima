import os
import json
import uuid
import httpx
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

# Variables de entorno
MASTER_URL = os.getenv("MASTER_URL", "http://master:8000")
BLOCK_SIZE_LIMIT = int(os.getenv("BLOCK_SIZE_MB", 1)) * 1024 * 1024 # Convertido a Bytes
WORKER_ID = f"worker_{uuid.uuid4().hex[:6]}"
STORAGE_DIR = "/app/storage"

# Estado interno
current_block_id = f"blk_{uuid.uuid4().hex}"
current_block_size = 0

os.makedirs(STORAGE_DIR, exist_ok=True)

app = FastAPI(title=f"DataNode - {WORKER_ID}")

async def seal_block_and_notify():
    global current_block_id, current_block_size
    
    # Avisar al Master (NameNode)
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{MASTER_URL}/register_block", json={
                "block_id": current_block_id,
                "worker_id": WORKER_ID,
                "size_bytes": current_block_size
            })
            print(f"[{WORKER_ID}] Bloque {current_block_id} sellado ({current_block_size} bytes).")
        except Exception as e:
            print(f"Error notificando al master: {e}")

    # Reiniciar para el siguiente bloque
    current_block_id = f"blk_{uuid.uuid4().hex}"
    current_block_size = 0

@app.post("/ingest")
async def ingest_data(request: Request):
    global current_block_id, current_block_size
    
    data = await request.json()
    data_str = json.dumps(data) + "\n"
    data_bytes = data_str.encode('utf-8')
    bytes_length = len(data_bytes)
    
    # Escribir en el archivo (Append)
    file_path = os.path.join(STORAGE_DIR, f"{current_block_id}.dat")
    with open(file_path, "ab") as f:
        f.write(data_bytes)
        
    current_block_size += bytes_length
    
    # ¿Superó el límite del bloque? (Ej: 1MB o 128MB)
    if current_block_size >= BLOCK_SIZE_LIMIT:
        await seal_block_and_notify()
        
    return {"status": "received"}

if __name__ == "__main__":
    import uvicorn
    # El puerto interno será 8001 para diferenciarlo del master si corren local
    uvicorn.run(app, host="0.0.0.0", port=8001)