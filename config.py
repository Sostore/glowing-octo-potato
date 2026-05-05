"""
Configuration management for the Pub/Sub system.
Loads settings from environment variables with sensible defaults.
"""
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """Application configuration."""
    
    # Flask Settings
    FLASK_HOST: str = "0.0.0.0"
    FLASK_PORT: int = 5000
    FLASK_DEBUG: bool = False
    
    # Storage Settings
    STORAGE_PATH: str = "./events_storage"
    
    # Processing Settings
    MAX_RETRIES: int = 3
    PROCESS_INTERVAL_SECONDS: float = 1.0
    
    # Logging Settings
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Firesec Settings
    DEFAULT_FIRESEC_TOPIC: str = "firesec.event"
    HEARTBEAT_TOPIC: str = "firesec.heartbeat"
    FIRE_EVENT_TOPIC: str = "firesec.event.fire"

    def __post_init__(self):
        """Resolve paths and override with environment variables."""
        # Resolve storage path relative to project root if not absolute
        storage_path = os.getenv("STORAGE_PATH", self.STORAGE_PATH)
        if not os.path.isabs(storage_path):
            # Assume relative to where the script is run or project root
            self.STORAGE_PATH = str(Path(storage_path).resolve())
        else:
            self.STORAGE_PATH = storage_path
            
        # Override other fields with environment variables if present
        self.FLASK_HOST = os.getenv("FLASK_HOST", self.FLASK_HOST)
        self.FLASK_PORT = int(os.getenv("FLASK_PORT", self.FLASK_PORT))
        self.FLASK_DEBUG = os.getenv("FLASK_DEBUG", "False").lower() in ("true", "1", "yes")
        
        self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", self.MAX_RETRIES))
        self.PROCESS_INTERVAL_SECONDS = float(os.getenv("PROCESS_INTERVAL_SECONDS", self.PROCESS_INTERVAL_SECONDS))
        
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", self.LOG_LEVEL)
        
        self.DEFAULT_FIRESEC_TOPIC = os.getenv("DEFAULT_FIRESEC_TOPIC", self.DEFAULT_FIRESEC_TOPIC)
        self.HEARTBEAT_TOPIC = os.getenv("HEARTBEAT_TOPIC", self.HEARTBEAT_TOPIC)
        self.FIRE_EVENT_TOPIC = os.getenv("FIRE_EVENT_TOPIC", self.FIRE_EVENT_TOPIC)


# Global config instance
config = Config()

def get_config() -> Config:
    """Get the global configuration instance."""
    return config
