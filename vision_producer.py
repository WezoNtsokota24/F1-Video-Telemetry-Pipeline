import cv2
import json
import time
import os
from ultralytics import YOLO
from confluent_kafka import Producer

# 1. CONFIGURATION 
TOPIC_NAME = "f1-vision-events"
# We accept generic vehicles because F1 cars look like "motorcycles" to AI
VALID_CLASSES = ['car', 'motorcycle', 'truck', 'bus', 'bicycle']

# Kafka Config
conf = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'f1-vision-producer'
}

print("🔌 Connecting to Redpanda...")
producer = Producer(conf)

# 2. SETUP VIDEO & MODEL 
video_path = os.path.join(os.path.dirname(__file__), "pitstop.mp4")

print("🧠 Loading AI Model...")
model = YOLO('yolov8n.pt') 

print(f"🎥 Loading Video from: {video_path}")
if not os.path.exists(video_path):
    print("❌ ERROR: 'pitstop.mp4' not found!")
    exit()

cap = cv2.VideoCapture(video_path)

# Define Pit Zone (Adjusted based on standard 1080p usually, but works for yours)
# If your boxes look wrong, tweak these numbers.
zone_x1, zone_y1 = 100, 100
zone_x2, zone_y2 = 1200, 900

pit_stop_active = False
start_time = 0

print(f"✅ Streaming to topic: {TOPIC_NAME} (Press 'q' to stop)")

while True:
    ret, frame = cap.read()
    if not ret: 
        print("End of video stream.")
        break

    # Get current frame number for the data payload
    frame_id = int(cap.get(cv2.CAP_PROP_POS_FRAMES))

    # Run Inference
    results = model(frame, stream=True) 

    # Draw Zone
    cv2.rectangle(frame, (zone_x1, zone_y1), (zone_x2, zone_y2), (255, 0, 0), 2)

    car_in_zone = False
    
    # We will build a list of detections to send to Kafka
    detections_payload = []

    for r in results:
        boxes = r.boxes
        for box in boxes:
            # 1. Get Coordinates & Label
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            cls_id = int(box.cls[0])
            label_name = model.names[cls_id]
            confidence = float(box.conf[0])

            # 2. Filter: Is this an F1 car (aka motorcycle)?
            if label_name in VALID_CLASSES:
                # Draw Box (Green)
                cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                cv2.putText(frame, f"F1 CAR ({label_name})", (x1, y1-10), 
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

                # Check Zone Logic
                center_x = (x1 + x2) / 2
                center_y = (y1 + y2) / 2
                if zone_x1 < center_x < zone_x2 and zone_y1 < center_y < zone_y2:
                    car_in_zone = True

                # Add to payload list
                detections_payload.append({
                    "object": label_name,
                    "confidence": confidence,
                    "bbox": [x1, y1, x2, y2]
                })

    # 3. BUSINESS LOGIC & SENDING 
    
    # Logic: If car is in zone, we flag it.
    event_type = "TRACK_ACTION"
    if car_in_zone:
        event_type = "PIT_ENTRY"
        if not pit_stop_active:
            pit_stop_active = True
            start_time = time.time()
            print("🏁 PIT STOP DETECTED - SENDING ALERT")
        
        duration = time.time() - start_time
        cv2.putText(frame, f"PIT TIMER: {duration:.2f}s", (50, 50), 
                    cv2.FONT_HERSHEY_SIMPLEX, 1.5, (0, 0, 255), 3)
    else:
        if pit_stop_active:
            print(f"🏁 PIT EXIT. Duration: {time.time() - start_time:.2f}s")
            pit_stop_active = False
            event_type = "PIT_EXIT"

    # Only send data if we actually saw something interesting (to save bandwidth)
    if len(detections_payload) > 0:
        kafka_data = {
            "frame_id": frame_id,
            "timestamp": time.time(),
            "event_type": event_type,
            "pit_timer": time.time() - start_time if pit_stop_active else 0,
            "detections": detections_payload
        }

        # SEND TO KAFKA
        producer.produce(
            TOPIC_NAME,
            key=str(frame_id),
            value=json.dumps(kafka_data)
        )
        producer.poll(0)

    cv2.imshow("Project Overwatch: Producer Feed", frame)

    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

producer.flush()
cap.release()
cv2.destroyAllWindows()