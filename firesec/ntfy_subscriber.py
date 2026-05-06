"""
ntfy.sh Subscriber Module

This module provides a subscriber implementation that sends notifications
to ntfy.sh using the requests library.

Usage:
    from firesec.ntfy_subscriber import NtfySubscriber
    
    subscriber = NtfySubscriber(
        server="https://ntfy.sh",
        topic="my-alerts"
    )
    
    # Use as a callback in PubSubCore
    core.subscribe("firesec.event.fire", subscriber.notify)
"""

import logging
import json
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)


class NtfySubscriber:
    """
    A subscriber that forwards events to ntfy.sh notification service.
    
    Uses the requests library to send HTTP POST requests to ntfy.sh
    with event details as notifications.
    """
    
    def __init__(
        self,
        server: str = "https://ntfy.sh",
        topic: str = "pubsub-events",
        priority: int = 3,
        tags: Optional[List[str]] = None,
        timeout: float = 10.0
    ):
        """
        Initialize the ntfy.sh subscriber.
        
        Args:
            server: ntfy.sh server URL (default: https://ntfy.sh)
            topic: The ntfy topic to publish notifications to
            priority: Notification priority (1=min, 2=low, 3=default, 4=high, 5=urgent)
            tags: List of emoji tags to include (e.g., ['warning', 'fire'])
            timeout: Request timeout in seconds
        """
        self.server = server.rstrip('/')
        self.topic = topic
        self.priority = priority
        self.tags = tags or []
        self.timeout = timeout
        self._session = requests.Session()
        
        logger.info(f"NtfySubscriber initialized: server={self.server}, topic={self.topic}")
    
    def notify(self, event) -> bool:
        """
        Send an event notification to ntfy.sh.
        
        This method is designed to be used as a callback for PubSubCore.subscribe().
        
        Args:
            event: The Event object to notify about
            
        Returns:
            True if notification was sent successfully, False otherwise
        """
        try:
            # Build notification title and message
            title = f"Event: {event.topic}"
            
            # Format the payload as the message body
            if isinstance(event.payload, dict):
                message = json.dumps(event.payload, indent=2)
            else:
                message = str(event.payload)
            
            # Add event metadata to the message
            metadata_info = []
            if event.id:
                metadata_info.append(f"ID: {event.id}")
            if event.attempts > 0:
                metadata_info.append(f"Attempts: {event.attempts}")
            
            if metadata_info:
                message = "\n".join(metadata_info) + "\n\n" + message
            
            # Determine tags based on event state
            tags = self.tags.copy()
            if event.state.value == "failed":
                tags.append("x")
            elif event.state.value == "completed":
                tags.append("white_check_mark")
            elif event.state.value == "pending":
                tags.append("hourglass")
            
            # Build request headers
            headers = {
                "Title": title,
                "Priority": str(self.priority),
            }
            
            if tags:
                headers["Tags"] = ",".join(tags)
            
            # Add click link if event ID is available
            if event.id:
                headers["Click"] = f"{self.server}/{self.topic}"
            
            # Send the notification
            url = f"{self.server}/{self.topic}"
            
            response = self._session.post(
                url,
                data=message.encode('utf-8'),
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code in (200, 201):
                logger.info(f"Ntfy notification sent successfully: topic={event.topic}, event_id={event.id}")
                return True
            else:
                logger.error(f"Ntfy notification failed: status={response.status_code}, response={response.text}")
                return False
                
        except requests.exceptions.Timeout:
            logger.error(f"Ntfy notification timeout: event_id={event.id if hasattr(event, 'id') else 'unknown'}")
            return False
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Ntfy connection error: {e}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Ntfy request error: {e}")
            return False
        except Exception as e:
            logger.exception(f"Unexpected error sending ntfy notification: {e}")
            return False
    
    def close(self) -> None:
        """Close the underlying requests session."""
        if self._session:
            self._session.close()
    
    def __del__(self):
        """Cleanup session on deletion."""
        try:
            self.close()
        except Exception:
            pass


def create_ntfy_subscriber(
    server: str = "https://ntfy.sh",
    topic: str = "pubsub-events",
    priority: int = 3,
    tags: Optional[List[str]] = None,
    timeout: float = 10.0
) -> NtfySubscriber:
    """
    Factory function to create an NtfySubscriber instance.
    
    Args:
        server: ntfy.sh server URL
        topic: The ntfy topic to publish notifications to
        priority: Notification priority (1-5)
        tags: List of emoji tags to include
        timeout: Request timeout in seconds
        
    Returns:
        Configured NtfySubscriber instance
    """
    return NtfySubscriber(
        server=server,
        topic=topic,
        priority=priority,
        tags=tags,
        timeout=timeout
    )
