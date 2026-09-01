# Event-Driven Notifications

Flask event producer and Redis Streams notification worker.

## Run

```bash
docker compose up --build
```

API: http://localhost:5000

Create an event:

```bash
curl -X POST http://localhost:5000/events -H "Content-Type: application/json" -d '{"type":"user.registered","user_id":"user-123","data":{"name":"Kunal"}}'
```

List notifications:

```bash
curl http://localhost:5000/notifications/user-123
```

Health:

```bash
curl http://localhost:5000/health
```
