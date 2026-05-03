from fastapi import FastAPI, HTTPException
from typing import List, Dict, Set
import random
import uuid
import httpx

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

@app.get("/get_block/{block_id}")
async def get_block(block_id: str):
    """Busca un bloque específico iterando por las réplicas hasta que una responda."""
    workers = filesystem_metadata.get(block_id)
    
    if not workers:
        raise HTTPException(status_code=404, detail="Metadatos del bloque no encontrados")
    
    async with httpx.AsyncClient() as client:
        for worker_host in workers:
            try:
                url = f"http://{worker_host}:8001/read_block/{block_id}"
                # Timeout corto para no esperar demasiado si el nodo está muerto
                response = await client.get(url, timeout=2.0) 
                
                if response.status_code == 200:
                    return {
                        "message": "Bloque recuperado exitosamente",
                        "source_worker": worker_host,
                        "data_preview": response.text[:50] + "... [truncado]" # Mostramos solo un fragmento
                    }
            except Exception as e:
                print(f"Nodo {worker_host} falló al recuperar bloque {block_id}: {e}")
                continue #Intenta con la siguiente réplica
                
    raise HTTPException(status_code=503, detail="Todas las réplicas del bloque están caídas")

@app.get("/calculate_average")
async def calculate_average(variable: str = "temperatura"):
    """Pide a los workers que sumen localmente y luego unifica los resultados."""
    if not filesystem_metadata:
        return {"message": "No hay bloques registrados en el sistema todavía"}
        
    global_sum = 0.0
    global_count = 0
    blocks_processed = 0
    
    async with httpx.AsyncClient() as client:
        # Iteramos sobre TODOS los bloques únicos
        for block_id, workers in filesystem_metadata.items():
            block_success = False
            
            for worker_host in workers:
                try:
                    url = f"http://{worker_host}:8001/process_block_average/{block_id}?variable={variable}"
                    response = await client.get(url, timeout=5.0)
                    
                    if response.status_code == 200:
                        result = response.json()
                        global_sum += result["sum"]
                        global_count += result["count"]
                        blocks_processed += 1
                        block_success = True
                        print(f"Bloque {block_id} procesado exitosamente por {worker_host}")
                        break 
                    
                except Exception as e:
                    print(f"Nodo {worker_host} falló al procesar {block_id}: {e}")
                    continue # El nodo falló, intentamos con la siguiente réplica
            
            if not block_success:
                print(f"CRÍTICO: No se pudo procesar el bloque {block_id}, todas sus réplicas fallaron.")

    if global_count == 0:
        return {"message": "No se encontraron datos válidos para calcular"}
        
    average = global_sum / global_count
    
    return {
        "variable": variable,
        "promedio": round(average, 2),
        "total_lecturas_analizadas": global_count,
        "bloques_procesados": blocks_processed
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)