"""
Execution Trajectory Model
"""
from dataclasses import dataclass, field, asdict
from typing import List, Set, Dict, Any, Tuple

@dataclass
class Trajectory:
    """
    Represents a symbolic execution path
    
    Attributes:
        path_id: Unique identifier
        basic_blocks: List of basic block addresses (e.g., [0x1000, 0x1010])
        path_condition: Logical formula (e.g., "x > 0 AND y < 100")
        branches_covered: Set of (source, dest) branch edges
        constraints: List of constraint strings
        cost: Estimated execution cost
        is_feasible: Whether path is satisfiable
    """
    path_id: str
    basic_blocks: List[int]
    path_condition: str
    branches_covered: Set[Tuple[int, int]] = field(default_factory=set)
    constraints: List[str] = field(default_factory=list)
    cost: float = 0.0
    is_feasible: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (JSON-serializable)"""
        return {
            'path_id': self.path_id,
            'basic_blocks': self.basic_blocks,
            'path_condition': self.path_condition,
            'branches_covered': [list(b) if isinstance(b, tuple) else b for b in self.branches_covered],
            'constraints': self.constraints,
            'cost': self.cost,
            'is_feasible': self.is_feasible
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Trajectory':
        """Create from dictionary"""
        # Convert list back to set
        if 'branches_covered' in data:
            data['branches_covered'] = set(
                tuple(branch) for branch in data['branches_covered']
            )
        return cls(**data)
    
    def __len__(self) -> int:
        """Length = number of basic blocks"""
        return len(self.basic_blocks)
    
    def __str__(self) -> str:
        return (
            f"Trajectory({self.path_id}, "
            f"length={len(self)}, "
            f"branches={len(self.branches_covered)}, "
            f"cost={self.cost:.2f})"
        )
