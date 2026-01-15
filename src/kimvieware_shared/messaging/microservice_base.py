"""
Base class for all microservices
"""
import pika
import time
from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime

from ..utils.rabbitmq import create_connection, declare_queue, publish_message, parse_message
from ..utils.logging import setup_logger, log_message_received, log_message_published

class MicroserviceBase(ABC):
    """Base microservice class"""
    
    def __init__(self, service_name: str, input_queue: str, output_queue: str,
                 rabbitmq_host: str = 'localhost', rabbitmq_port: int = 5672,
                 rabbitmq_user: str = 'admin', rabbitmq_pass: str = 'kimvie2025'):
        
        self.service_name = service_name
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_pass = rabbitmq_pass
        
        self.logger = setup_logger(service_name)
        self.connection = None
        self.channel = None
        
        self.logger.info(f"🚀 {service_name} initialized")
    
    def _connect(self):
        """Connect to RabbitMQ"""
        self.connection = create_connection(
            self.rabbitmq_host, self.rabbitmq_port,
            self.rabbitmq_user, self.rabbitmq_pass
        )
        self.channel = self.connection.channel()
        declare_queue(self.channel, self.input_queue)
        declare_queue(self.channel, self.output_queue)
    
    @abstractmethod
    def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Override this in subclasses"""
        pass
    
    def _callback(self, ch, method, properties, body):
        """Message callback"""
        job_id = "unknown"
        start_time = time.time()
        
        try:
            message = parse_message(body)
            if message is None:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            
            job_id = message.get('job_id', 'unknown')
            log_message_received(self.logger, job_id, self.input_queue)
            
            result = self.process_message(message)
            
            duration = time.time() - start_time
            if 'metadata' not in result:
                result['metadata'] = {}
            result['metadata'].update({
                'processing_time_ms': int(duration * 1000),
                'processed_by': self.service_name,
                'processed_at': datetime.utcnow().isoformat() + 'Z'
            })
            
            publish_message(self.channel, self.output_queue, result)
            log_message_published(self.logger, job_id, self.output_queue)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.info(f"✅ Job {job_id} completed in {duration:.2f}s")
            
        except Exception as e:
            self.logger.error(f"❌ Job {job_id} failed: {str(e)}")
            error_msg = {
                'job_id': job_id,
                'status': 'failed',
                'error': str(e),
                'phase': self.service_name,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            try:
                publish_message(self.channel, self.output_queue, error_msg)
            except:
                pass
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def start(self):
        """Start consuming"""
        self.logger.info(f"🎬 Starting {self.service_name}...")
        self._connect()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.input_queue, on_message_callback=self._callback)
        self.logger.info(f"✨ Ready! Waiting on '{self.input_queue}'...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop gracefully"""
        if self.channel:
            self.channel.stop_consuming()
        if self.connection:
            self.connection.close()
        self.logger.info(f"👋 {self.service_name} stopped")
