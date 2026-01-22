import cv2
from ultralytics import YOLO
import time

model = YOLO('yolov8n.pt')
cap = cv2.VideoCapture(0) # Keep using your webcam for now

# --- LEARNING MOMENT: The Pit Box ---
# These are X,Y coordinates. (Adjust these to fit where you sit!)
# Imagine a square in the middle of your screen.
zone_x1, zone_y1 = 100, 100
zone_x2, zone_y2 = 500, 500

pit_stop_active = False
start_time = 0

while True:
    ret, frame = cap.read()
    if not ret: break

    # We only want to detect 'car' (2) or 'person' (0) for testing
    results = model(frame, classes=[0, 2], stream=True) 

    # Draw the "Pit Box" zone on the screen so we can see it
    cv2.rectangle(frame, (zone_x1, zone_y1), (zone_x2, zone_y2), (255, 0, 0), 2)
    cv2.putText(frame, "PIT BOX ZONE", (zone_x1, zone_y1-10), 
                cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 0, 0), 2)

    for r in results:
        boxes = r.boxes
        for box in boxes:
            # Get coordinates of the detected object
            x1, y1, x2, y2 = box.xyxy[0]
            x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)

            # Check if the object's center is inside our Pit Box
            center_x = (x1 + x2) / 2
            center_y = (y1 + y2) / 2

            if zone_x1 < center_x < zone_x2 and zone_y1 < center_y < zone_y2:
                if not pit_stop_active:
                    pit_stop_active = True
                    start_time = time.time()
                    print("🏁 PIT STOP STARTED!")
                
                # Calculate current duration
                duration = time.time() - start_time
                cv2.putText(frame, f"STOP TIME: {duration:.2f}s", (x1, y1-10), 
                            cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0, 255, 0), 2)
            else:
                if pit_stop_active:
                    print(f"🏁 PIT STOP COMPLETE: {time.time() - start_time:.2f}s")
                    pit_stop_active = False

    cv2.imshow("Project Overwatch: Pit Strategy Engine", frame)

    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()