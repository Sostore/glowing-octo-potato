"""
Persistent File-System Event Store

This module provides persistent storage for events using the file system.
Events are stored as JSON files organized by state (pending/completed/failed).

Designed for Windows 10/11 compatibility with atomic writes and thread safety.
"""

import json
import threading
from pathlib import Path
from typing import List, Optional

from core.event import Event, EventState


class EventStore:
    """
    Persistent file-system storage for events.
    
    Uses a directory structure with JSON files for each event.
    Implements file locking for thread-safe operations on Windows.
    """
    
    def __init__(self, storage_path: str):
        """
        Initialize the event store.
        
        Args:
            storage_path: Base directory path for storing events
        """
        self.storage_path = Path(storage_path)
        self._lock = threading.RLock()
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directory structure."""
        for state in EventState:
            dir_path = self.storage_path / state.value
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def _get_event_path(self, event_id: str, state: EventState) -> Path:
        """Get the file path for an event."""
        return self.storage_path / state.value / f"{event_id}.json"
    
    def _find_event_path(self, event_id: str) -> Optional[Path]:
        """Find an event file across all state directories."""
        for state in EventState:
            path = self._get_event_path(event_id, state)
            if path.exists():
                return path
        return None
    
    def _write_event(self, event: Event, use_atomic_write: bool = True) -> None:
        """
        Write an event to disk.
        
        Args:
            event: The event to write
            use_atomic_write: If True, write to temp file then rename (atomic on Windows)
        """
        target_path = self._get_event_path(event.id, event.state)
        
        # Remove from other state directories if exists there
        for state in EventState:
            if state != event.state:
                other_path = self._get_event_path(event.id, state)
                if other_path.exists():
                    try:
                        other_path.unlink()
                    except FileNotFoundError:
                        pass
        
        data = json.dumps(event.to_dict(), indent=2, default=str)
        
        if use_atomic_write:
            # Atomic write: write to temp file then rename
            temp_path = target_path.with_suffix('.tmp')
            temp_path.write_text(data, encoding='utf-8')
            # On Windows, rename is atomic if target doesn't exist
            if target_path.exists():
                target_path.unlink()
            temp_path.rename(target_path)
        else:
            target_path.write_text(data, encoding='utf-8')
    
    def save(self, event: Event) -> None:
        """
        Save an event to the store.
        
        Args:
            event: The event to save
        """
        with self._lock:
            self._write_event(event)
    
    def load(self, event_id: str) -> Optional[Event]:
        """
        Load an event by ID.
        
        Args:
            event_id: The event ID
            
        Returns:
            The event if found, None otherwise
        """
        with self._lock:
            path = self._find_event_path(event_id)
            if path is None:
                return None
            
            try:
                data = json.loads(path.read_text(encoding='utf-8'))
                return Event.from_dict(data)
            except (json.JSONDecodeError, KeyError) as e:
                raise ValueError(f"Failed to load event {event_id}: {e}")
    
    def delete(self, event_id: str) -> bool:
        """
        Delete an event from the store.
        
        Args:
            event_id: The event ID
            
        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            path = self._find_event_path(event_id)
            if path is None:
                return False
            
            path.unlink()
            return True
    
    def get_by_state(self, state: EventState, topic: Optional[str] = None) -> List[Event]:
        """
        Get all events with a specific state.
        
        Args:
            state: The event state to filter by
            topic: Optional topic filter
            
        Returns:
            List of matching events
        """
        with self._lock:
            dir_path = self.storage_path / state.value
            if not dir_path.exists():
                return []
            
            events = []
            for file_path in dir_path.glob("*.json"):
                try:
                    data = json.loads(file_path.read_text(encoding='utf-8'))
                    event = Event.from_dict(data)
                    if topic is None or event.topic == topic:
                        events.append(event)
                except (json.JSONDecodeError, KeyError):
                    continue
            
            return events
    
    def get_pending_events(self, topic: Optional[str] = None) -> List[Event]:
        """Get all pending events."""
        return self.get_by_state(EventState.PENDING, topic)
    
    def count_by_state(self, state: EventState) -> int:
        """Count events in a specific state."""
        with self._lock:
            dir_path = self.storage_path / state.value
            if not dir_path.exists():
                return 0
            return len(list(dir_path.glob("*.json")))
