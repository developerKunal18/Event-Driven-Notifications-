import json,os,time,uuid
import redis

client=redis.Redis.from_url(os.getenv("REDIS_URL","redis://redis:6379/0"),decode_responses=True)
STREAM="day316:events"
GROUP="notification-workers"
CONSUMER=os.getenv("CONSUMER_NAME",f"worker-{uuid.uuid4()}")
PREFIX="day316:notification:"

def setup():
    try:
        client.xgroup_create(STREAM,GROUP,id="0",mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise

def message_for(event_type):
    return {
        "user.registered":"Welcome! Your account has been created.",
        "order.created":"Your order has been created.",
        "payment.completed":"Your payment was completed successfully.",
    }.get(event_type,f"New event received: {event_type}")

def process(message_id,fields):
    user_id=fields["user_id"]
    notification_id=str(uuid.uuid4())
    item={
        "id":notification_id,
        "user_id":user_id,
        "event_id":fields["event_id"],
        "type":fields["type"],
        "message":message_for(fields["type"]),
        "created_at":time.time(),
        "status":"sent",
    }
    client.set(f"{PREFIX}{user_id}:{notification_id}",json.dumps(item))
    client.xack(STREAM,GROUP,message_id)
    print(f"Notification created: {notification_id}",flush=True)

def run():
    setup()
    print(f"Worker started: {CONSUMER}",flush=True)
    while True:
        try:
            result=client.xreadgroup(GROUP,CONSUMER,{STREAM:">"},count=10,block=5000)
            for _,messages in result or []:
                for message_id,fields in messages:
                    try:
                        process(message_id,fields)
                    except Exception as exc:
                        print(f"Message failed: {exc}",flush=True)
        except redis.RedisError as exc:
            print(f"Redis error: {exc}",flush=True)
            time.sleep(3)

if __name__=="__main__":
    run()
