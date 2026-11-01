
import json
import random
import time
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Sample library books
books = [
    ("BK001", "Database Management Systems"),
    ("BK002", "Computer Networks"),
    ("BK003", "Operating Systems"),
    ("BK004", "Data Structures and Algorithms"),
    ("BK005", "Machine Learning"),
    ("BK006", "Artificial Intelligence"),
    ("BK007", "Big Data Analytics"),
]

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("==============================================")
print("     LIBRARY KAFKA EVENT PRODUCER")
print("==============================================")
print("Generating real-time library events...")
print("Press Ctrl+C to stop.\n")

try:
    while True:

        # Generate student information
        student_id = f"23ISE{random.randint(1, 100):03d}"
        student_name = fake.name()

        # Select a random book
        book_id, book_name = random.choice(books)

        # Generate a library action
        action = random.choices(
            ["BORROW", "RETURN"],
            weights=[60, 40],
            k=1
        )[0]

        # Create event
        event = {
            "student_id": student_id,
            "student_name": student_name,
            "book_id": book_id,
            "book_name": book_name,
            "action": action,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Publish event to Kafka topic
        future = producer.send(
            "library-events",
            value=event
        )

        # Make sure Kafka receives the event
        metadata = future.get(timeout=10)

        producer.flush()

        print("✅ Event sent to Kafka")
        print(f"Topic     : {metadata.topic}")
        print(f"Partition : {metadata.partition}")
        print(f"Offset    : {metadata.offset}")
        print(f"Student   : {student_id}")
        print(f"Book      : {book_name}")
        print(f"Action    : {action}")
        print(f"Time      : {event['timestamp']}")
        print("-" * 60)

        # Generate next event after 3 seconds
        time.sleep(3)

except KeyboardInterrupt:
    print("\n\nProducer stopped by user.")

finally:
    producer.close()
    print("Kafka producer connection closed.")
