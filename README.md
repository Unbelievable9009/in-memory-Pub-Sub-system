# In-Memory Pub/Sub System

This is a simplified in-memory Publish/Subscribe system implemented in Python using FastAPI. It supports WebSocket communication for pub/sub operations and HTTP REST APIs for management.

## Features

*   **WebSocket Endpoint (`/ws`):**
    *   `subscribe`: Subscribe to a topic.
    *   `unsubscribe`: Unsubscribe from a topic.
    *   `publish`: Publish a message to a topic.
    *   `ping`: Client health check.
*   **HTTP REST Endpoints:**
    *   `POST /topics`: Create a new topic.
    *   `DELETE /topics/{name}`: Delete a topic and disconnect subscribers.
    *   `GET /topics`: List all topics and their subscriber counts.
    *   `GET /health`: System health and basic stats.
    *   `GET /stats`: Detailed stats per topic (message counts, subscribers).
*   **In-Memory:** All state (topics, subscriptions, messages) is held in memory and not persisted across restarts.
*   **Concurrency Safe:** Designed to handle multiple publishers and subscribers concurrently using `asyncio` and locks.
*   **Message Fan-out:** Messages published to a topic are delivered to all current subscribers of that topic.
*   **Topic Isolation:** Messages are isolated to their respective topics.
*   **Message Replay:** Supports replaying the last `n` messages to new subscribers via the `last_n` parameter in the subscribe message. Up to 100 messages are stored per topic.
*   **Heartbeats:** Server sends periodic "ping" info messages to connected WebSocket clients.

## Assumptions & Design Choices

*   **Backpressure Policy:** Each subscriber has a bounded queue of 50 messages. If the queue is full when a new message arrives:
    *   The **oldest** message in the queue is dropped.
    *   A `SLOW_CONSUMER` error message is sent to the subscriber (best-effort).
    *   This prevents a slow consumer from consuming excessive memory or blocking the publisher.
*   **Message History:** Each topic stores a maximum of 100 recent messages in a ring buffer for the `last_n` replay feature.
*   **UUIDs for Messages:** Published messages are expected to have a valid UUID in the `message.id` field.
*   **No Authentication:** The system does not implement any authentication for REST or WebSocket endpoints.
*   **Graceful Shutdown:** On shutdown, the server attempts to notify subscribers of topic deletions and close WebSocket connections.

## Prerequisites

*   Docker

## Running with Docker

1.  **Build the Docker Image:**
    ```bash
    docker build -t inmemory-pubsub .
    ```

2.  **Run the Docker Container:**
    ```bash
    docker run -d -p 8000:8000 --name pubsub-app inmemory-pubsub
    ```
    The application will be accessible on `http://localhost:8000`.

## API Usage

### REST Endpoints

*   **Create Topic:**
    ```bash
    curl -X POST http://localhost:8000/topics -H "Content-Type: application/json" -d '{"name": "orders"}'
    ```

*   **List Topics:**
    ```bash
    curl http://localhost:8000/topics
    ```

*   **Delete Topic:**
    ```bash
    curl -X DELETE http://localhost:8000/topics/orders
    ```

*   **Health Check:**
    ```bash
    curl http://localhost:8000/health
    ```

*   **Stats:**
    ```bash
    curl http://localhost:8000/stats
    ```

### WebSocket Endpoint (`ws://localhost:8000/ws`)

Use a WebSocket client (e.g., JavaScript in a browser, `wscat`, Python `websocket-client`) to interact. Messages are JSON.

*   **Subscribe:**
    ```json
    {
      "type": "subscribe",
      "topic": "orders",
      "client_id": "s1",
      "last_n": 5,
      "request_id": "sub-123"
    }
    ```

*   **Unsubscribe:**
    ```json
    {
      "type": "unsubscribe",
      "topic": "orders",
      "client_id": "s1",
      "request_id": "unsub-456"
    }
    ```

*   **Publish:**
    ```json
    {
      "type": "publish",
      "topic": "orders",
      "message": {
        "id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
        "payload": {
          "order_id": "ORD-789",
          "amount": "12.34",
          "currency": "USD"
        }
      },
      "request_id": "pub-789"
    }
    ```

*   **Ping:**
    ```json
    {
      "type": "ping",
      "request_id": "ping-abc"
    }
    ```

**Server -> Client Message Examples:** See the project description for `ack`, `event`, `error`, `pong`, and `info` message formats.
