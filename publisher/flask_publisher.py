"""
Flask Publisher for FireSec Events

This Flask application listens for POST requests on '/firesec-sink'
and publishes events to the PubSub core based on XML payload analysis.
Produces topics: firesec.event.fire and firesec.heartbeat
"""

import os
import sys
import logging
from flask import Flask, request, jsonify

# Add the parent directory to the path to import core modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pubsub_core import PubSubCore
from firesec.parsing import parse_firesec_xml, validate_firesec_xml
from config import get_config

# Load configuration
config = get_config()

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize the PubSub core with configured storage path
pubsub_core = PubSubCore(storage_path=config.STORAGE_PATH, max_retries=config.MAX_RETRIES)


@app.route('/firesec-sink', methods=['POST'])
def firesec_sink():
    """
    Endpoint to receive FireSec XML events and publish them to the PubSub core.

    Expected payload: Raw XML content
    Content-Type: application/xml or text/xml
    
    The XML is parsed to determine the topic:
    - firesec.event.fire: For fire-related events
    - firesec.heartbeat: For heartbeat/status events

    Returns event details on success.
    """
    logger.info("Received POST request to /firesec-sink")
    
    # Get raw XML content
    xml_content = request.get_data(as_text=True)
    
    if not xml_content:
        logger.warning("No XML content provided in request")
        return jsonify({
            'status': 'error',
            'message': 'No XML content provided'
        }), 400
    
    logger.debug(f"Received XML payload:\n{xml_content}")
    
    # Validate XML format
    if not validate_firesec_xml(xml_content):
        logger.error("Invalid XML format received")
        return jsonify({
            'status': 'error',
            'message': 'Invalid XML format'
        }), 400
    
    # Parse XML to determine topic and extract payload
    topic, payload = parse_firesec_xml(xml_content)
    
    logger.info(f"Parsed XML: topic='{topic}', payload={payload}")
    
    try:
        # Publish the event to the core
        event = pubsub_core.publish(
            topic=topic,
            payload=payload,
            metadata={
                'source': 'firesec-sink',
                'content_type': 'application/xml'
            }
        )

        logger.info(f"Event published successfully: id={event.id}, topic={topic}, state={event.state.value}")

        return jsonify({
            'status': 'success',
            'message': f'Event published to topic: {topic}',
            'event_id': event.id,
            'topic': event.topic,
            'state': event.state.value
        }), 201

    except Exception as e:
        logger.exception(f"Error publishing event: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    stats = pubsub_core.get_stats()
    return jsonify({
        'status': 'healthy',
        'service': 'flask-publisher',
        'stats': stats
    }), 200


@app.route('/stats', methods=['GET'])
def get_stats():
    """Get current event statistics."""
    stats = pubsub_core.get_stats()
    return jsonify({
        'status': 'success',
        'stats': stats
    }), 200


if __name__ == '__main__':
    logger.info(f"Starting Flask Publisher on {config.FLASK_HOST}:{config.FLASK_PORT}")
    logger.info(f"Storage path: {config.STORAGE_PATH}")
    logger.info(f"Endpoint: POST /firesec-sink (accepts XML)")
    logger.info(f"Topics produced: {config.FIRE_EVENT_TOPIC}, {config.HEARTBEAT_TOPIC}")

    # Start the background processor
    pubsub_core.start_processor(interval=config.PROCESS_INTERVAL_SECONDS)

    try:
        app.run(host=config.FLASK_HOST, port=config.FLASK_PORT, debug=config.FLASK_DEBUG, threaded=True)
    finally:
        pubsub_core.stop_processor()
