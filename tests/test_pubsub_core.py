"""
Test suite for the Pub/Sub Core system.
"""

import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.pubsub_core import PubSubCore, EventState, create_pubsub


def test_basic_publish_subscribe():
    """Test basic publish and subscribe functionality."""
    print("Testing basic publish/subscribe...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        core = create_pubsub(tmpdir)
        
        received_events = []
        
        def handler(event):
            received_events.append(event)
            return True
        
        core.subscribe("test-topic", handler)
        event = core.publish("test-topic", {"data": "hello"})
        
        assert event.topic == "test-topic"
        assert event.payload == {"data": "hello"}
        assert len(received_events) == 1
        assert received_events[0].id == event.id
        
        # Check event state is completed
        loaded_event = core.get_event(event.id)
        assert loaded_event.state == EventState.COMPLETED
        
        print("✓ Basic publish/subscribe passed")


def test_persistent_storage():
    """Test that events are persisted to disk."""
    print("Testing persistent storage...")
    
    tmpdir = tempfile.mkdtemp()
    try:
        core = create_pubsub(tmpdir)
        
        # Publish an event (will be processed and moved to completed)
        event = core.publish("persist-test", {"value": 42})
        
        # Verify file exists in completed directory (since it was processed)
        state_dir = Path(tmpdir) / "completed"
        files = list(state_dir.glob("*.json"))
        assert len(files) == 1
        
        # Create new core instance (simulating restart)
        core2 = create_pubsub(tmpdir)
        loaded = core2.get_event(event.id)
        
        assert loaded is not None
        assert loaded.payload == {"value": 42}
        assert loaded.topic == "persist-test"
        
        print("✓ Persistent storage passed")
    finally:
        shutil.rmtree(tmpdir)


def test_event_states():
    """Test event state transitions."""
    print("Testing event states...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        core = create_pubsub(tmpdir, max_retries=2)
        
        # Test successful processing
        def success_handler(event):
            return True
        
        core.subscribe("success-topic", success_handler)
        event1 = core.publish("success-topic", {})
        assert core.get_event(event1.id).state == EventState.COMPLETED
        
        # Test failed processing - first attempt
        fail_count = [0]
        
        def fail_handler(event):
            fail_count[0] += 1
            return False
        
        core.subscribe("fail-topic", fail_handler)
        event2 = core.publish("fail-topic", {})
        
        # First attempt: stays PENDING for retry (attempts=1)
        assert core.get_event(event2.id).state == EventState.PENDING
        assert fail_count[0] == 1
        
        # Second attempt via process_pending - should now be FAILED (max_retries=2)
        core.process_pending()
        assert core.get_event(event2.id).state == EventState.FAILED
        assert fail_count[0] == 2
        
        print("✓ Event states passed")


def test_retry_failed():
    """Test retrying failed events."""
    print("Testing retry failed events...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        core = create_pubsub(tmpdir, max_retries=1)
        
        # Create a failed event
        def always_fail(event):
            return False
        
        core.subscribe("retry-topic", always_fail)
        event = core.publish("retry-topic", {"retry": True})
        
        assert core.get_event(event.id).state == EventState.FAILED
        
        # Change handler to succeed
        def now_succeeds(event):
            return True
        
        core.unsubscribe("retry-topic", always_fail)
        core.subscribe("retry-topic", now_succeeds)
        
        # Retry failed events
        count = core.retry_failed()
        assert count == 1
        
        # Process pending
        core.process_pending()
        
        assert core.get_event(event.id).state == EventState.COMPLETED
        
        print("✓ Retry failed events passed")


def test_multiple_subscribers():
    """Test multiple subscribers to same topic."""
    print("Testing multiple subscribers...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        core = create_pubsub(tmpdir)
        
        results = {"sub1": False, "sub2": False}
        
        def subscriber1(event):
            results["sub1"] = True
            return True
        
        def subscriber2(event):
            results["sub2"] = True
            return True
        
        core.subscribe("multi-topic", subscriber1)
        core.subscribe("multi-topic", subscriber2)
        
        core.publish("multi-topic", {})
        
        assert results["sub1"]
        assert results["sub2"]
        
        print("✓ Multiple subscribers passed")


def test_background_processor():
    """Test background event processor."""
    print("Testing background processor...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        core = create_pubsub(tmpdir)
        
        processed = []
        
        def handler(event):
            processed.append(event.id)
            return True
        
        core.subscribe("bg-topic", handler)
        core.start_processor(poll_interval=0.1)
        
        # Publish while processor is running
        event = core.publish("bg-topic", {"background": True})
        
        # Wait for processing
        time.sleep(0.3)
        
        assert len(processed) >= 1
        assert event.id in processed
        
        core.stop_processor()
        
        print("✓ Background processor passed")


def test_stats():
    """Test statistics retrieval."""
    print("Testing statistics...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        core = create_pubsub(tmpdir, max_retries=1)
        
        # Create events in different states
        core.subscribe("stats-success", lambda e: True)
        core.subscribe("stats-fail", lambda e: False)
        
        core.publish("stats-success", {})  # Will complete
        core.publish("stats-success", {})  # Will complete
        core.publish("stats-fail", {})     # Will fail
        
        stats = core.get_stats()
        
        assert stats['completed'] == 2
        assert stats['failed'] == 1
        
        print("✓ Statistics passed")


def test_topic_filtering():
    """Test filtering events by topic."""
    print("Testing topic filtering...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        core = create_pubsub(tmpdir)
        
        # Use handlers that don't auto-complete (no subscribers during publish)
        # First, let's check completed events by topic instead
        
        results = {"a": 0, "b": 0}
        
        def handler_a(event):
            results["a"] += 1
            return True
        
        def handler_b(event):
            results["b"] += 1
            return True
        
        core.subscribe("topic-a", handler_a)
        core.subscribe("topic-b", handler_b)
        
        core.publish("topic-a", {"source": "a"})
        core.publish("topic-b", {"source": "b"})
        core.publish("topic-a", {"source": "a2"})
        
        # Check completed events by topic
        completed_a = core.store.get_by_state(EventState.COMPLETED, topic="topic-a")
        completed_b = core.store.get_by_state(EventState.COMPLETED, topic="topic-b")
        
        assert len(completed_a) == 2
        assert len(completed_b) == 1
        
        print("✓ Topic filtering passed")


def run_all_tests():
    """Run all tests."""
    print("=" * 50)
    print("Running Pub/Sub Core Tests")
    print("=" * 50)
    
    test_basic_publish_subscribe()
    test_persistent_storage()
    test_event_states()
    test_retry_failed()
    test_multiple_subscribers()
    test_background_processor()
    test_stats()
    test_topic_filtering()
    
    print("=" * 50)
    print("All tests passed! ✓")
    print("=" * 50)


if __name__ == "__main__":
    run_all_tests()
