import json
import pika
from datetime import datetime 
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="URL Browser Service")

class URLRequest(BaseModel):
    url: str

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "url_queue")

def get_rabbitmq_connection():
    """Connection with RabbitMQ"""
    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise

@app.on_event("startup")
async def startup_event():
    """Init function"""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        connection.close()
        logger.info("RabbitMQ queue initialized")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ queue: {e}")

@app.post("/browse")
async def browse_url(request: URLRequest):
    """Endpoint to add url into queue"""
    try:
        if not request.url.startswith(('http://', 'https://')):
            raise HTTPException(status_code=400, detail="Invalid URL format")
        
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        message = {
            'url': request.url,
            'timestamp': str(datetime.now())
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  
            )
        )
        
        connection.close()
        
        logger.info(f"URL added to queue: {request.url}")
        
        return {
            "status": "success", 
            "message": f"URL {request.url} added to processing queue"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "fastapi-app"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)