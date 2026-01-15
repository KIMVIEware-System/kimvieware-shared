"""
RabbitMQ Utility Functions
"""
import pika
import json
import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def create_connection(
    host: str = 'localhost',
    port: int = 5672,
    username: str = 'admin',
    password: str = 'kimvie2025',
    max_retries: int = 5,
    retry_delay: int = 5
) -> pika.BlockingConnection:
    """Create RabbitMQ connection with retry logic"""
    
    for attempt in range(1, max_retries + 1):
        try:
            credentials = pika.PlainCredentials(username, password)
            parameters = pika.ConnectionParameters(
                host=host,
                port=port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            connection = pika.BlockingConnection(parameters)
            logger.info(f" Connected to RabbitMQ at {host}:{port}")
            return connection
        except Exception as e:
            logger.error(f" Attempt {attempt}/{max_retries} failed: {str(e)}")
            if attempt < max_retries:
                logger.info(f" Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to connect after {max_retries} attempts")

def declare_queue(channel, queue_name: str, durable: bool = True):
    """Declare a queue"""
    channel.queue_declare(queue=queue_name, durable=durable)

def publish_message(channel, queue_name: str, message: Dict[str, Any], persistent: bool = True):
    """Publish message to queue"""
    properties = pika.BasicProperties(
        delivery_mode=2 if persistent else 1,
        content_type='application/json'
    )
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(message),
        properties=properties
    )

def parse_message(body: bytes) -> Optional[Dict[str, Any]]:
    """Parse JSON message"""
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None
