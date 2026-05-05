"""
Event definitions for the Pub/Sub system.

This module contains the EventState enum and Event dataclass
used throughout the pub/sub system.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass, field


class EventState(Enum):
    """Possible states for an event."""
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Event:
    """Represents an event in the pub/sub system."""
    id: str
    topic: str
    payload: Any
    state: EventState
    created_at: float
    updated_at: float
    attempts: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """Convert event to dictionary for serialization."""
        return {
            'id': self.id,
            'topic': self.topic,
            'payload': self.payload,
            'state': self.state.value,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'attempts': self.attempts,
            'error_message': self.error_message,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Event':
        """Create an Event from a dictionary."""
        return cls(
            id=data['id'],
            topic=data['topic'],
            payload=data['payload'],
            state=EventState(data['state']),
            created_at=data['created_at'],
            updated_at=data['updated_at'],
            attempts=data.get('attempts', 0),
            error_message=data.get('error_message'),
            metadata=data.get('metadata', {})
        )
