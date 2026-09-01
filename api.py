import json,os,time,uuid
import redis
from flask import Flask,jsonify,request

app=Flask(__name__)
client=redis.Redis.from_url(os.getenv("REDIS_URL","redis://redis:6379/0"),decode_responses=True)
STREAM="day316:events"
PREFIX="day316:notification:"

@app.get("/")
def home():
    return jsonify(service="day316-event-notifications",message="Event API is running")

@app.get("/health")
def health():
    try:
        client.ping()
        return jsonify(status="healthy",redis="connected")
    except redis.RedisError:
        return jsonify(status="unhealthy"),503

@app.post("/events")
def create_event():
    data=request.get_json(silent=True) or {}
    event_type=data.get("type")
    user_id=data.get("user_id")
    if not event_type or not user_id:
        return jsonify(error="type and user_id are required"),400

    event_id=str(uuid.uuid4())
    client.xadd(STREAM,{
        "event_id":event_id,
        "type":event_type,
        "user_id":user_id,
        "data":json.dumps(data.get("data",{})),
        "created_at":str(time.time()),
    },maxlen=10000,approximate=True)

    return jsonify(event_id=event_id,status="queued"),202

@app.get("/notifications/<user_id>")
def notifications(user_id):
    result=[]
    for key in client.scan_iter(match=PREFIX+user_id+":*"):
        raw=client.get(key)
        if raw:
            result.append(json.loads(raw))
    result.sort(key=lambda x:x.get("created_at",0),reverse=True)
    return jsonify(notifications=result)

if __name__=="__main__":
    app.run(host="0.0.0.0",port=5000)
