"""
Job Message Model
"""
from dataclasses import dataclass, field, asdict
from typing import Dict, Any
from datetime import datetime

@dataclass
class JobMessage:
    """
    Standard message format for inter-service communication
    
    Attributes:
        job_id: Unique job identifier (UUID)
        status: Current job status
        timestamp: ISO 8601 timestamp
        data: Payload data (flexible)
        metadata: Processing metadata
    """
    job_id: str
    status: str
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobMessage':
        """Create from dictionary"""
        return cls(**data)
    
    def __str__(self) -> str:
        return f"JobMessage(job_id={self.job_id}, status={self.status})"
