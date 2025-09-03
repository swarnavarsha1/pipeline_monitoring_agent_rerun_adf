# shared/schemas.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class FailureContext:
    pipeline_name: str
    run_id: str
    status: str
    error_message: str
    failed_activity: Optional[str]
    timestamp: datetime
    knowledge_text: Optional[str] = None