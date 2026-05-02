from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict

app = FastAPI(title="NameNode - Master")

filesystem_metadata: Dict[str, List[str]] = {}

class BlockReport(BaseModel):
    block_id: str
    worker_id: str
    size_bytes: int

@app.post("/register_block")
async def register_block(report: BlockReport):
    if report.block_id not in filesystem_metadata:
        filesystem_metadata[report.block_id] = []
    
    if report.worker_id not in filesystem_metadata[report.block_id]:
        filesystem_metadata[report.block_id].append(report.worker_id)
        
    return {"status": "ok", "message": f"Metadatos actualizados para {report.block_id}"}

@app.get("/metadata")
async def get_metadata():
    return {"total_blocks": len(filesystem_metadata), "metadata": filesystem_metadata}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)