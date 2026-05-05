"""
FireSec XML Parsing Module

This module handles parsing of FireSec XML payloads.
Currently contains dummy implementation for topic extraction.
"""

from typing import Tuple, Optional, Dict, Any
import xml.etree.ElementTree as ET


def parse_firesec_xml(xml_content: str) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Parse FireSec XML content and extract topic and payload data.
    
    Args:
        xml_content: Raw XML string from FireSec system
        
    Returns:
        Tuple of (topic, payload_dict) where:
        - topic: 'firesec.event.fire' or 'firesec.heartbeat'
        - payload_dict: Dictionary containing parsed event data
    """
    # DUMMY IMPLEMENTATION
    # In production, this would parse actual FireSec XML schema
    
    try:
        root = ET.fromstring(xml_content)
        
        # Dummy logic to determine topic based on XML structure
        # Look for common FireSec elements (dummy detection)
        event_type = None
        
        # Check for fire-related elements (dummy)
        fire_indicators = ['fire', 'smoke', 'heat', 'flame', 'alarm']
        heartbeat_indicators = ['heartbeat', 'status', 'ping', 'alive']
        
        root_tag = root.tag.lower() if root.tag else ''
        text_content = (root.text or '').lower()
        
        # Simple dummy detection logic
        is_heartbeat = any(ind in root_tag or ind in text_content 
                          for ind in heartbeat_indicators)
        is_fire = any(ind in root_tag or ind in text_content 
                     for ind in fire_indicators)
        
        if is_heartbeat:
            topic = 'firesec.heartbeat'
        elif is_fire:
            topic = 'firesec.event.fire'
        else:
            # Default to fire event if unclear
            topic = 'firesec.event.fire'
        
        # Convert XML to dictionary (dummy conversion)
        payload_dict = {
            'raw_xml': xml_content,
            'root_element': root.tag,
            'detected_topic': topic,
            'attributes': dict(root.attrib) if root.attrib else {}
        }
        
        return topic, payload_dict
        
    except ET.ParseError as e:
        # Return generic fire event on parse error
        return 'firesec.event.fire', {
            'raw_xml': xml_content,
            'parse_error': str(e),
            'detected_topic': 'firesec.event.fire'
        }
    except Exception as e:
        # Return generic fire event on any error
        return 'firesec.event.fire', {
            'raw_xml': xml_content,
            'error': str(e),
            'detected_topic': 'firesec.event.fire'
        }


def validate_firesec_xml(xml_content: str) -> bool:
    """
    Validate if the XML content is well-formed.
    
    Args:
        xml_content: Raw XML string
        
    Returns:
        True if valid XML, False otherwise
    """
    try:
        ET.fromstring(xml_content)
        return True
    except ET.ParseError:
        return False
