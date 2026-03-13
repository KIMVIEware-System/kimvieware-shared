"""
Job Storage - MongoDB backend for detailed results
"""
from pymongo import MongoClient
from datetime import datetime, timezone # Import timezone
from typing import Dict, Any, Optional
import os
import logging # Import logging

logger = logging.getLogger(__name__)

def _serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively convert datetime objects in a dictionary to ISO 8601 strings."""
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z') # Ensure UTC and 'Z' suffix
        elif isinstance(value, dict):
            doc[key] = _serialize_doc(value)
        elif isinstance(value, list):
            doc[key] = [_serialize_doc(item) if isinstance(item, dict) else item for item in value]
    return doc

class JobStorage:
    """Store and retrieve job results with full details"""
    
    def __init__(self):
        mongo_host = os.getenv('MONGO_HOST', 'localhost')
        mongo_user = os.getenv('MONGO_USER', 'admin')
        mongo_pass = os.getenv('MONGO_PASS', 'kimvie2025')
        
        self.mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/"
        
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client.kimvieware
        self.jobs = self.db.jobs
        
        try:
            self.jobs.create_index("job_id", unique=True)
            self.jobs.create_index("created_at")
            logger.info("MongoDB indexes ensured for 'jobs' collection.")
        except Exception as e:
            logger.warning(f"⚠️  JobStorage: Index creation skipped: {e}")
    
    def save_job(self, job_data: Dict[str, Any]):
        """Save or update job data"""
        job_id = job_data['job_id']
        
        if 'created_at' not in job_data:
            job_data['created_at'] = datetime.now(timezone.utc)
        job_data['updated_at'] = datetime.now(timezone.utc)
        
        self.jobs.update_one(
            {'job_id': job_id},
            {'$set': job_data},
            upsert=True
        )
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job by ID, converting datetime objects to ISO 8601 strings."""
        doc = self.jobs.find_one({'job_id': job_id}, {'_id': 0})
        if doc:
            return _serialize_doc(doc)
        return None
    
    def get_all_jobs(self, limit: int = 50) -> list:
        """Get recent jobs, converting datetime objects to ISO 8601 strings."""
        docs = list(self.jobs.find(
            {},
            {'_id': 0}
        ).sort('created_at', -1).limit(limit))
        return [_serialize_doc(doc) for doc in docs]
    
    def update_phase(self, job_id: str, phase: str, data: Dict[str, Any]):
        """Update specific phase data"""
        self.jobs.update_one(
            {'job_id': job_id},
            {
                '$set': {
                    f'phases.{phase}': data,
                    'updated_at': datetime.now(timezone.utc)
                }
            }
        )

