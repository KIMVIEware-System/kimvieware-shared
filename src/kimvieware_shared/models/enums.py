"""
Enumerations for KIMVIEware
"""
from enum import Enum

class JobStatus(str, Enum):
    """Job lifecycle status"""
    # Phase 0
    SUBMITTED = "submitted"
    VALIDATING = "validating"
    VALIDATED = "validated"
    VALIDATION_FAILED = "validation_failed"
    
    # Phase 1
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    EXTRACTION_FAILED = "extraction_failed"
    
    # Phase 2
    REDUCING = "reducing"
    REDUCED = "reduced"
    REDUCTION_FAILED = "reduction_failed"
    
    # Phase 3
    OPTIMIZING = "optimizing"
    OPTIMIZED = "optimized"
    OPTIMIZATION_FAILED = "optimization_failed"
    
    # Phase 4
    EXECUTING = "executing"
    COMPLETED = "completed"
    EXECUTION_FAILED = "execution_failed"
    
    # Global
    FAILED = "failed"

class Language(str, Enum):
    """Supported programming languages"""
    PYTHON = "python"
    C = "c"
    CPP = "cpp"
    JAVA = "java"
    UNKNOWN = "unknown"
