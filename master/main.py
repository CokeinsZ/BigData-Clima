from fastapi import FastAPI, HTTPException
from typing import List, Dict, Set
import random
import uuid

app = FastAPI(title="NameNode")

active_workers: Set[str] = set() # Directorio de trabajadores activos (sus Hostnames exactos)
filesystem_metadata: Dict[str, List[str]] = {} # Registro de dónde está cada bloque


@app.post("/register_worker")
async def register_worker(worker_hostname: str):
    active_workers.add(worker_hostname)
    return {"status": "ok", "message": f"Worker {worker_hostname} registrado."}

@app.get("/allocate_block")
async def allocate_block(replicas: int = 3):
    """El servicio de ingesta pide los nodos donde guardar un bloque nuevo."""
    if len(active_workers) == 0:
        raise HTTPException(status_code=503, detail="No hay workers disponibles")
    
    # Elegir aleatoriamente los nodos según el factor de replicación
    num_replicas = min(replicas, len(active_workers))
    chosen_workers = random.sample(list(active_workers), k=num_replicas)
    
    new_block_id = f"blk_{uuid.uuid4().hex}"
    filesystem_metadata[new_block_id] = chosen_workers    # Guardamos en los metadatos a los se le asignó el bloque

    return {
        "block_id": new_block_id,
        "target_workers": chosen_workers
    }

@app.get("/metadata")
async def get_metadata():
    return {
        "total_active_workers": len(active_workers),
        "workers": list(active_workers),
        "blocks_metadata": filesystem_metadata
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)