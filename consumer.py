import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "library-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="library-consumer-group",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


print("Library Kafka Consumer started...")
print("Waiting for library events...\n")


for message in consumer:

    event = message.value

    print("New Library Event Received")
    print("----------------------------------------")

    print(
        "Student ID :",
        event.get("student_id")
    )

    print(
        "Book ID    :",
        event.get("book_id")
    )

    print(
        "Book Name  :",
        event.get("book_name")
    )

    print(
        "Action     :",
        event.get("action")
    )

    print("----------------------------------------")
