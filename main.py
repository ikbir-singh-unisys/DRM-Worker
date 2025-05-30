# worker/main.py
from fastapi import Depends, Request, FastAPI, BackgroundTasks
from pydantic import BaseModel
from core.processor import DRMProcessor
import logging
import uvicorn
from concurrent.futures import ThreadPoolExecutor

import socket
import platform

app = FastAPI()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


executor = ThreadPoolExecutor(max_workers=4)

class JobData(BaseModel):
    job_id: str
    content_id: str
    client_id: str
    s3_input_id: str
    s3_output_id: str
    is_paid: bool
    upload_to_s3: bool
    s3_source: str
    s3_destination: str
    already_transcoded: bool

@app.get("/")
def start():
    return { "status": "Server is Running" }
    
    
@app.get("/health")
def health():
    return {"status": "ok"}
    
   
@app.get("/test-info")
def get_machine_info(request: Request):
    client_ip = request.headers.get("x-forwarded-for") or request.client.host
    hostname = socket.gethostname()
    machine_ip = socket.gethostbyname(hostname)
    os_info = platform.system()
    arch = platform.machine()

    return {
        "client_ip": client_ip,        
        "hostname": hostname,         
        "machine_ip": machine_ip,      
        "os": os_info,                 
        "arch": arch                
    }


@app.post("/api/run-job")
async def run_job(job: JobData, background_tasks: BackgroundTasks):
    logging.info(f"📥 Received job: {job.job_id}")
    processor = DRMProcessor()
    # background_tasks.add_task(processor.process, job)
    executor.submit(processor.process, job)  # run in parallel thread
    return {"message": "Job received and is being processed"}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10200)
