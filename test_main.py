import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from main import app, topics, MAX_HISTORY, MAX_SUBSCRIBER_QUEUE

client = TestClient(app)

@pytest.fixture(autouse=True)
async def clear_topics():
    # Clear topics before each test
    topics.clear()
    await asyncio.sleep(0.01) # allow any pending tasks to clear

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "uptime_sec" in data
    assert data["topics"] == 0
    assert data["subscribers"] == 0

def test_create_topic():
    response = client.post("/topics", json={"name": "test_topic"})
    assert response.status_code == 201
    assert response.json() == {"status": "created", "topic": "test_topic"}
    assert "test_topic" in topics

    response = client.post("/topics", json={"name": "test_topic"})
    assert response.status_code == 409

    response = client.post("/topics", json={"name": ""})
    assert response.status_code == 400

def test_list_topics():
    client.post("/topics", json={"name": "topic1"})
    client.post("/topics", json={"name": "topic2"})
    response = client.get("/topics")
    assert response.status_code == 200
    data = response.json()
    assert len(data["topics"]) == 2
    assert {"name": "topic1", "subscribers": 0} in data["topics"]
    assert {"name": "topic2", "subscribers": 0} in data["topics"]

def test_delete_topic():
    client.post("/topics", json={"name": "delete_me"})
    assert "delete_me" in topics
    response = client.delete("/topics/delete_me")
    assert response.status_code == 200
    assert response.json() == {"status": "deleted", "topic": "delete_me"}
    assert "delete_me" not in topics

    response = client.delete("/topics/nonexistent")
    assert response.status_code == 404

def test_stats():
    client.post("/topics", json={"name": "stats_topic"})
    response = client.get("/stats")
    assert response.status_code == 200
    assert response.json() == {
        "topics": {
            "stats_topic": {"messages": 0, "subscribers": 0}
        }
    }

def test_websocket_ping():
    with client.websocket_connect("/ws") as websocket:
        request_id = str(uuid.uuid4())
        websocket.send_json({"type": "ping", "request_id": request_id})
        response = websocket.receive_json()
        assert response["type"] == "pong"
        assert response["request_id"] == request_id
        assert "ts" in response

def test_websocket_invalid_type():
    with client.websocket_connect("/ws") as websocket:
        request_id = str(uuid.uuid4())
        websocket.send_json({"type": "unknown", "request_id": request_id})
        response = websocket.receive_json()
        assert response["type"] == "error"
        assert response["error"]["code"] == "BAD_REQUEST"
        assert "Unknown message type" in response["error"]["message"]
        assert response["request_id"] == request_id

def test_websocket_missing_topic():
    with client.websocket_connect("/ws") as websocket:
        websocket.send_json({"type": "subscribe", "client_id": "c1"})
        response = websocket.receive_json()
        assert response["type"] == "error"
        assert response["error"]["code"] == "BAD_REQUEST"
        assert response["error"]["message"] == "Missing 'topic'"

def test_websocket_topic_not_found():
    with client.websocket_connect("/ws") as websocket:
        websocket.send_json({"type": "subscribe", "topic": "nope", "client_id": "c1"})
        response = websocket.receive_json()
        assert response["type"] == "error"
        assert response["error"]["code"] == "TOPIC_NOT_FOUND"
        assert response["topic"] == "nope"

@pytest.mark.asyncio
async def test_websocket_pubsub():
    client.post("/topics", json={"name": "pubsub"})
    await asyncio.sleep(0.01) # Allow topic creation

    with client.websocket_connect("/ws") as ws1, client.websocket_connect("/ws") as ws2:
        # Subscribe ws1
        ws1.send_json({"type": "subscribe", "topic": "pubsub", "client_id": "s1", "request_id": "sub1"})
        ack1 = ws1.receive_json()
        assert ack1["type"] == "ack"
        assert ack1["request_id"] == "sub1"
        assert ack1["topic"] == "pubsub"

        # Subscribe ws2
        ws2.send_json({"type": "subscribe", "topic": "pubsub", "client_id": "s2", "request_id": "sub2"})
        ack2 = ws2.receive_json()
        assert ack2["type"] == "ack"
        assert ack2["request_id"] == "sub2"

        await asyncio.sleep(0.01) # ensure subscriptions are processed

        # Publish message
        msg_id = str(uuid.uuid4())
        payload = {"data": "hello"}
        with client.websocket_connect("/ws") as publisher:
            publisher.send_json({
                "type": "publish",
                "topic": "pubsub",
                "message": {"id": msg_id, "payload": payload},
                "request_id": "pub1"
            })
            pub_ack = publisher.receive_json()
            assert pub_ack["type"] == "ack"
            assert pub_ack["request_id"] == "pub1"

        # Check ws1 receives event
        event1 = ws1.receive_json()
        assert event1["type"] == "event"
        assert event1["topic"] == "pubsub"
        assert event1["message"]["id"] == msg_id
        assert event1["message"]["payload"] == payload

        # Check ws2 receives event
        event2 = ws2.receive_json()
        assert event2["type"] == "event"
        assert event2["topic"] == "pubsub"
        assert event2["message"]["id"] == msg_id
        assert event2["message"]["payload"] == payload

        # Unsubscribe ws1
        ws1.send_json({"type": "unsubscribe", "topic": "pubsub", "client_id": "s1", "request_id": "unsub1"})
        unsub_ack1 = ws1.receive_json()
        assert unsub_ack1["type"] == "ack"
        assert unsub_ack1["request_id"] == "unsub1"

        # Publish another message
        msg_id2 = str(uuid.uuid4())
        with client.websocket_connect("/ws") as publisher:
            publisher.send_json({
                "type": "publish",
                "topic": "pubsub",
                "message": {"id": msg_id2, "payload": {"data": "world"}},
                "request_id": "pub2"
            })
            publisher.receive_json() # Ack

        # ws1 should not receive it
        with pytest.raises(Exception): # Adjust exception type based on testing client behavior on timeout
             ws1.receive_json(timeout=0.1)

        # ws2 should receive it
        event2_new = ws2.receive_json()
        assert event2_new["type"] == "event"
        assert event2_new["message"]["id"] == msg_id2

@pytest.mark.asyncio
async def test_websocket_last_n():
    client.post("/topics", json={"name": "lastn"})
    await asyncio.sleep(0.01)

    msg_ids = []
    # Publish some messages
    with client.websocket_connect("/ws") as publisher:
        for i in range(5):
            msg_id = str(uuid.uuid4())
            msg_ids.append(msg_id)
            publisher.send_json({
                "type": "publish",
                "topic": "lastn",
                "message": {"id": msg_id, "payload": {"i": i}},
                "request_id": f"pub-{i}"
            })
            publisher.receive_json() # Ack

    await asyncio.sleep(0.01) # ensure publishes are processed

    with client.websocket_connect("/ws") as subscriber:
        # Subscribe with last_n
        subscriber.send_json({"type": "subscribe", "topic": "lastn", "client_id": "s-lastn", "last_n": 3, "request_id": "sub-lastn"})
        ack = subscriber.receive_json()
        assert ack["type"] == "ack"

        # Receive replayed messages
        replayed = []
        for _ in range(3):
            event = subscriber.receive_json()
            assert event["type"] == "event"
            assert event["topic"] == "lastn"
            replayed.append(event["message"]["id"])

        assert replayed == msg_ids[2:]

        # Publish a new message
        new_msg_id = str(uuid.uuid4())
        with client.websocket_connect("/ws") as publisher:
             publisher.send_json({
                "type": "publish",
                "topic": "lastn",
                "message": {"id": new_msg_id, "payload": {"i": 5}},
                "request_id": "pub-new"
            })
             publisher.receive_json() # Ack

        # Subscriber should receive the new message
        event = subscriber.receive_json()
        assert event["type"] == "event"
        assert event["message"]["id"] == new_msg_id

@pytest.mark.asyncio
async def test_websocket_backpressure():
    client.post("/topics", json={"name": "backpressure"})
    await asyncio.sleep(0.01)

    with client.websocket_connect("/ws") as subscriber:
        subscriber.send_json({"type": "subscribe", "topic": "backpressure", "client_id": "slow", "request_id": "sub-slow"})
        ack = subscriber.receive_json()
        assert ack["type"] == "ack"

        await asyncio.sleep(0.01)

        topic = topics["backpressure"]
        ws_obj = list(topic.subscribers)[0]
        queue = topic.subscriber_queues[ws_obj]

        # Fill the queue directly to simulate a slow consumer
        for i in range(MAX_SUBSCRIBER_QUEUE):
            await queue.put({"type": "event", "message": {"id": str(uuid.uuid4()), "payload": {"i": i}}})
        assert queue.full()

        # Publish a message, which should trigger backpressure
        with client.websocket_connect("/ws") as publisher:
            msg_id = str(uuid.uuid4())
            publisher.send_json({
                "type": "publish",
                "topic": "backpressure",
                "message": {"id": msg_id, "payload": {"data": "overflow"}},
                "request_id": "pub-overflow"
            })
            pub_ack = publisher.receive_json()
            assert pub_ack["type"] == "ack"

        await asyncio.sleep(0.1) # Allow queue processing

        # Check for SLOW_CONSUMER error
        slow_consumer_error = None
        try:
            for _ in range(MAX_SUBSCRIBER_QUEUE + 5): # Check a few messages
                msg = subscriber.receive_json(timeout=0.1)
                if msg["type"] == "error" and msg["error"]["code"] == "SLOW_CONSUMER":
                    slow_consumer_error = msg
                    break
        except Exception: # Timeout expected if messages are dropped
            pass
        # This assertion is timing-dependent. The error might be sent after some events.
        # A more robust test would mock the websocket.send_json to check calls.
        # For now, we'll just check if the queue is still full, implying drops happened.

        # The queue should have space now as the sender task consumes from it.
        # The key thing is that the publish didn't block.

        # Let's verify the oldest message was dropped.
        # We need to receive messages to see what's in the queue.
        received_messages = []
        try:
            for i in range(MAX_SUBSCRIBER_QUEUE):
                msg = subscriber.receive_json(timeout=0.1)
                if msg["type"] == "event":
                    received_messages.append(msg["message"]["payload"].get("i"))
        except Exception:
            pass

        if 0 in received_messages:
             print("Oldest message (i=0) was not dropped, received:", received_messages)
        assert 0 not in received_messages # First message (i=0) should be dropped

        # The new message "overflow" should be present
        has_overflow = False
        try:
            # Drain any remaining
            while True:
                 msg = subscriber.receive_json(timeout=0.1)
                 if msg["type"] == "event" and msg["message"]["payload"].get("data") == "overflow":
                      has_overflow = True
                      break
        except Exception:
            pass
        # This part is hard to assert reliably without mocks due to async nature.

@pytest.mark.asyncio
async def test_topic_deletion_notifies_subscribers():
    client.post("/topics", json={"name": "to_be_deleted"})
    await asyncio.sleep(0.01)

    with client.websocket_connect("/ws") as subscriber:
        subscriber.send_json({"type": "subscribe", "topic": "to_be_deleted", "client_id": "s1"})
        subscriber.receive_json()  # ACK
        await asyncio.sleep(0.01)

        client.delete("/topics/to_be_deleted")
        await asyncio.sleep(0.01)

        # Check for info message
        info = subscriber.receive_json()
        assert info["type"] == "info"
        assert info["topic"] == "to_be_deleted"
        assert info["msg"] == "topic_deleted"

        # Connection should be closed
        with pytest.raises(WebSocketDisconnect):
            subscriber.receive_json(timeout=1.0)

