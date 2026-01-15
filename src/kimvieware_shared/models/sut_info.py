"""
System Under Test Information Model
"""
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class SUTInfo:
    """
    Information about a System Under Test
    
    Attributes:
        language: Detected programming language
        size_bytes: Total size of source files
        files_count: Number of source files
        entry_point: Main file (e.g., main.py, main.c)
        checksum: SHA256 checksum of all files
    """
    language: str
    size_bytes: int
    files_count: int
    entry_point: Optional[str] = None
    checksum: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SUTInfo':
        """Create from dictionary"""
        return cls(**data)
    
    def __str__(self) -> str:
        return (
            f"SUTInfo(language={self.language}, "
            f"files={self.files_count}, "
            f"size={self.size_bytes} bytes)"
        )
