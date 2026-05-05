"""
Pub/Sub Event System with Persistent File-System Storage

This module provides a publish/subscribe pattern implementation with events
persisted to the file system. Events can be in one of three states:
pending, completed, or failed.

Designed for Windows 10/11 compatibility.
"""

import json
import os
import uuid
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Any

from core.event import Event, EventState
from core.event_store import EventStore


class PubSubCore:
    """
    Core pub/sub system with persistent event storage.
    
    Features:
    - Publish events to topics
    - Subscribe callbacks to topics
    - Persistent storage of all events
    - Event state management (pending/completed/failed)
    - Thread-safe operations
    - Retry support for failed events
    """
    
    def __init__(self, storage_path: str, max_retries: int = 3):
        """
        Initialize the pub/sub core.
        
        Args:
            storage_path: Directory path for persistent event storage
            max_retries: Maximum retry attempts for failed events
        """
        self.store = EventStore(storage_path)
        self._subscribers: Dict[str, List[Callable[[Event], bool]]] = {}
        self._lock = threading.RLock()
        self._max_retries = max_retries
        self._running = False
        self._processor_thread: Optional[threading.Thread] = None
    
    def subscribe(self, topic: str, callback: Callable[[Event], bool]) -> None:
        """
        Subscribe a callback to a topic.
        
        Args:
            topic: The topic to subscribe to
            callback: Function to call when event is published.
                     Should return True for success, False for failure.
        """
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = []
            self._subscribers[topic].append(callback)
    
    def unsubscribe(self, topic: str, callback: Callable[[Event], bool]) -> bool:
        """
        Unsubscribe a callback from a topic.
        
        Args:
            topic: The topic to unsubscribe from
            callback: The callback to remove
            
        Returns:
            True if callback was removed, False if not found
        """
        with self._lock:
            if topic not in self._subscribers:
                return False
            
            if callback in self._subscribers[topic]:
                self._subscribers[topic].remove(callback)
                return True
            return False
    
    def publish(self, topic: str, payload: Any, 
                metadata: Optional[Dict[str, Any]] = None) -> Event:
        """
        Publish an event to a topic.
        
        Args:
            topic: The topic to publish to
            payload: The event payload (must be JSON-serializable)
            metadata: Optional metadata dictionary
            
        Returns:
            The created event
        """
        now = time.time()
        event = Event(
            id=str(uuid.uuid4()),
            topic=topic,
            payload=payload,
            state=EventState.PENDING,
            created_at=now,
            updated_at=now,
            metadata=metadata or {}
        )
        
        self.store.save(event)
        
        # Process the event (immediate processing)
        self._process_event(event)
        
        return event
    
    def _process_event(self, event: Event) -> None:
        """
        Process a single event by calling all subscribers.
        
        Args:
            event: The event to process
        """
        if event.state != EventState.PENDING:
            return
        
        with self._lock:
            callbacks = self._subscribers.get(event.topic, []).copy()
        
        if not callbacks:
            # No subscribers, mark as completed
            event.state = EventState.COMPLETED
            event.updated_at = time.time()
            self.store.save(event)
            return
        
        event.attempts += 1
        all_success = True
        error_msg = None
        
        for callback in callbacks:
            try:
                success = callback(event)
                if not success:
                    all_success = False
                    error_msg = "Callback returned False"
            except Exception as e:
                all_success = False
                error_msg = str(e)
                break
        
        event.updated_at = time.time()
        
        if all_success:
            event.state = EventState.COMPLETED
        elif event.attempts >= self._max_retries:
            event.state = EventState.FAILED
            event.error_message = error_msg
        # else: stays PENDING for retry
        
        self.store.save(event)
    
    def process_pending(self, topic: Optional[str] = None) -> int:
        """
        Process all pending events.
        
        Args:
            topic: Optional topic filter
            
        Returns:
            Number of events processed
        """
        pending = self.store.get_pending_events(topic)
        for event in pending:
            self._process_event(event)
        return len(pending)
    
    def retry_failed(self, topic: Optional[str] = None) -> int:
        """
        Retry all failed events (resets attempts and sets to pending).
        
        Args:
            topic: Optional topic filter
            
        Returns:
            Number of events retried
        """
        failed = self.store.get_by_state(EventState.FAILED, topic)
        count = 0
        
        for event in failed:
            event.state = EventState.PENDING
            event.attempts = 0
            event.error_message = None
            event.updated_at = time.time()
            self.store.save(event)
            count += 1
        
        return count
    
    def get_event(self, event_id: str) -> Optional[Event]:
        """Get an event by ID."""
        return self.store.load(event_id)
    
    def get_stats(self) -> Dict[str, int]:
        """Get event statistics by state."""
        return {
            'pending': self.store.count_by_state(EventState.PENDING),
            'completed': self.store.count_by_state(EventState.COMPLETED),
            'failed': self.store.count_by_state(EventState.FAILED)
        }
    
    def start_processor(self, poll_interval: float = 1.0) -> None:
        """
        Start background event processor thread.
        
        Args:
            poll_interval: Seconds between processing cycles
        """
        if self._running:
            return
        
        self._running = True
        
        def processor_loop():
            while self._running:
                self.process_pending()
                time.sleep(poll_interval)
        
        self._processor_thread = threading.Thread(target=processor_loop, daemon=True)
        self._processor_thread.start()
    
    def stop_processor(self) -> None:
        """Stop the background event processor."""
        self._running = False
        if self._processor_thread:
            self._processor_thread.join(timeout=5.0)
            self._processor_thread = None
    
    def clear_all(self) -> None:
        """Clear all events from storage."""
        with self._lock:
            for state in EventState:
                dir_path = self.store.storage_path / state.value
                for file_path in dir_path.glob("*.json"):
                    file_path.unlink()


# Convenience function for creating a PubSubCore instance
def create_pubsub(storage_path: str, max_retries: int = 3) -> PubSubCore:
    """
    Create a new PubSubCore instance.
    
    Args:
        storage_path: Directory path for persistent event storage
        max_retries: Maximum retry attempts for failed events
        
    Returns:
        Configured PubSubCore instance
    """
    return PubSubCore(storage_path, max_retries)
