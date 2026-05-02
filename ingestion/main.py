import os
import json
import httpx
import asyncio
from fastapi import FastAPI, Request

MASTER_URL = os.getenv("MASTER_URL", "http://master:8000")
BLOCK_SIZE_LIMIT = int(os.getenv("BLOCK_SIZE_MB", 1)) * 1024 * 1024
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", 3))

app = FastAPI(title="Ingestion Service")

current_buffer = bytearray()
buffer_lock = asyncio.Lock()

async def distribute_block_task(block_data: bytearray):
    # Lógica separada para no bloquear la ingesta mientras se transmite a los workers
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{MASTER_URL}/allocate_block?replicas={REPLICATION_FACTOR}")
            allocation = response.json()
            block_id = allocation["block_id"]
            target_workers = allocation["target_workers"]
            
            print(f"Master asignó el bloque {block_id} a los nodos: {target_workers}")
            
            for worker_host in target_workers:
                worker_url = f"http://{worker_host}:8001/store_block/{block_id}"
                await client.post(worker_url, content=bytes(block_data))
                print(f"-> Bloque {block_id} enviado a {worker_host}")
                
        except Exception as e:
            print(f"Error en la distribución del bloque: {e}")

@app.post("/ingest")
async def receive_data(request: Request):
    global current_buffer
    
    data = await request.json()
    data_bytes = (json.dumps(data) + "\n").encode('utf-8')
    
    async with buffer_lock:
        current_buffer.extend(data_bytes)
        
        # Si llegamos al límite de bloque, sellamos y distribuimos
        if len(current_buffer) >= BLOCK_SIZE_LIMIT:
            # Creamos una copia rápida del buffer y la limpiamos para no bloquear nuevas lecturas
            block_data = current_buffer.copy()
            current_buffer.clear()
            
            # Ejecutamos la distribución en segundo plano
            asyncio.create_task(distribute_block_task(block_data))

    return {"status": "buffered"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)