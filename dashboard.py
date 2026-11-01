
import json
import time
from collections import Counter

import streamlit as st
from kafka import KafkaConsumer


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Library Kafka Monitor",
    page_icon="📚",
    layout="wide"
)

st.title("📚 Library Real-Time Monitoring System")
st.caption("Apache Kafka • Python • Streamlit")


# ============================================================
# KAFKA CONSUMER
# ============================================================

@st.cache_resource
def create_consumer():

    return KafkaConsumer(
        "library-events",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="library-dashboard-group",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        request_timeout_ms=10000
    )


consumer = create_consumer()


# ============================================================
# SESSION STATE
# ============================================================

if "events" not in st.session_state:
    st.session_state.events = []

if "latest_offset" not in st.session_state:
    st.session_state.latest_offset = None

if "latest_partition" not in st.session_state:
    st.session_state.latest_partition = None


# ============================================================
# READ EVENTS FROM KAFKA
# ============================================================

records = consumer.poll(
    timeout_ms=1000,
    max_records=20
)

for topic_partition, messages in records.items():

    for message in messages:

        event = message.value

        st.session_state.events.append(event)

        st.session_state.latest_offset = message.offset

        st.session_state.latest_partition = message.partition


# Keep latest 100 events
st.session_state.events = st.session_state.events[-100:]

all_events = st.session_state.events


# ============================================================
# KAFKA CONNECTION STATUS
# ============================================================

try:

    partitions = consumer.partitions_for_topic("library-events")

    kafka_connected = partitions is not None

except Exception:

    kafka_connected = False


# ============================================================
# KAFKA DATA PIPELINE
# ============================================================

st.subheader("🔄 Kafka Data Pipeline")

pipeline1, pipeline2, pipeline3, pipeline4 = st.columns(4)


with pipeline1:

    st.success("🟢 PRODUCER")

    st.write("**Python Event Generator**")

    st.caption(
        "Generates library transactions"
    )


with pipeline2:

    st.success("🟢 KAFKA")

    st.write("**Apache Kafka**")

    st.caption(
        "Topic: library-events"
    )


with pipeline3:

    st.success("🟢 CONSUMER")

    st.write("**Python Kafka Consumer**")

    st.caption(
        "Reads events from Kafka"
    )


with pipeline4:

    st.success("🟢 DASHBOARD")

    st.write("**Streamlit**")

    st.caption(
        "Real-time analytics"
    )


# ============================================================
# KAFKA STATUS
# ============================================================

st.subheader("⚡ Kafka Status")

status_col1, status_col2, status_col3, status_col4 = st.columns(4)


with status_col1:

    if kafka_connected:

        st.metric(
            "Kafka Broker",
            "🟢 Connected"
        )

    else:

        st.metric(
            "Kafka Broker",
            "🔴 Disconnected"
        )


with status_col2:

    st.metric(
        "Kafka Topic",
        "library-events"
    )


with status_col3:

    if st.session_state.latest_partition is not None:

        st.metric(
            "Partition",
            st.session_state.latest_partition
        )

    else:

        st.metric(
            "Partition",
            "-"
        )


with status_col4:

    if st.session_state.latest_offset is not None:

        st.metric(
            "Latest Offset",
            st.session_state.latest_offset
        )

    else:

        st.metric(
            "Latest Offset",
            "-"
        )


st.divider()


# ============================================================
# REAL-TIME FILTERS
# ============================================================

st.subheader("🔎 Real-Time Filters")

filter_col1, filter_col2, filter_col3 = st.columns(3)


# ------------------------------------------------------------
# ACTION FILTER
# ------------------------------------------------------------

with filter_col1:

    action_filter = st.selectbox(
        "📖 Action",
        [
            "ALL",
            "BORROW",
            "RETURN"
        ]
    )


# ------------------------------------------------------------
# BOOK FILTER
# ------------------------------------------------------------

book_list = sorted(
    {
        event.get("book_name")
        for event in all_events
        if event.get("book_name")
    }
)


with filter_col2:

    book_filter = st.selectbox(
        "📚 Book",
        ["ALL"] + book_list
    )


# ------------------------------------------------------------
# STUDENT FILTER
# ------------------------------------------------------------

student_list = sorted(
    {
        event.get("student_id")
        for event in all_events
        if event.get("student_id")
    }
)


with filter_col3:

    student_filter = st.selectbox(
        "👨‍🎓 Student",
        ["ALL"] + student_list
    )


# ============================================================
# APPLY FILTERS
# ============================================================

filtered_events = []


for event in all_events:

    # Action filter
    if (
        action_filter != "ALL"
        and event.get("action") != action_filter
    ):
        continue

    # Book filter
    if (
        book_filter != "ALL"
        and event.get("book_name") != book_filter
    ):
        continue

    # Student filter
    if (
        student_filter != "ALL"
        and event.get("student_id") != student_filter
    ):
        continue

    filtered_events.append(event)


events = filtered_events


# ============================================================
# FILTER RESULT
# ============================================================

st.caption(
    f"Showing {len(events)} matching events "
    f"out of {len(all_events)} total events"
)


st.divider()


# ============================================================
# ANALYTICS
# ============================================================

total_events = len(events)


borrow_count = sum(
    1
    for event in events
    if event.get("action") == "BORROW"
)


return_count = sum(
    1
    for event in events
    if event.get("action") == "RETURN"
)


# ============================================================
# BOOK ANALYTICS
# ============================================================

borrowed_books = Counter(
    event.get("book_name")
    for event in events
    if event.get("action") == "BORROW"
)


# ============================================================
# STUDENT ANALYTICS
# ============================================================

student_activity = Counter(
    event.get("student_id")
    for event in events
)


# ============================================================
# MOST BORROWED BOOK
# ============================================================

if borrowed_books:

    most_borrowed_book, most_borrowed_count = (
        borrowed_books.most_common(1)[0]
    )

else:

    most_borrowed_book = "No data"


# ============================================================
# LATEST EVENT
# ============================================================

if events:

    latest_event = events[-1]

else:

    latest_event = None


# ============================================================
# LIVE STATISTICS
# ============================================================

st.subheader("📊 Live Statistics")

col1, col2, col3, col4 = st.columns(4)


with col1:

    st.metric(
        "Total Events",
        total_events
    )


with col2:

    st.metric(
        "📖 Borrowed",
        borrow_count
    )


with col3:

    st.metric(
        "↩️ Returned",
        return_count
    )


with col4:

    st.metric(
        "📚 Most Borrowed",
        most_borrowed_book
    )


st.divider()


# ============================================================
# BORROW VS RETURN
# ============================================================

st.subheader("📈 Borrow vs Return")


action_data = {
    "BORROW": borrow_count,
    "RETURN": return_count
}


st.bar_chart(action_data)


st.divider()


# ============================================================
# TOP BOOKS + TOP STUDENTS
# ============================================================

col1, col2 = st.columns(2)


# ============================================================
# TOP BOOKS
# ============================================================

with col1:

    st.subheader("📚 Top 5 Most Borrowed Books")

    top_books = dict(
        borrowed_books.most_common(5)
    )

    if top_books:

        st.bar_chart(top_books)

    else:

        st.info(
            "No borrowing data for selected filters."
        )


# ============================================================
# TOP STUDENTS
# ============================================================

with col2:

    st.subheader("👨‍🎓 Top 5 Active Students")

    top_students = dict(
        student_activity.most_common(5)
    )

    if top_students:

        st.bar_chart(top_students)

    else:

        st.info(
            "No student activity for selected filters."
        )


st.divider()


# ============================================================
# LATEST TRANSACTION
# ============================================================

st.subheader("🔴 Latest Transaction")


if latest_event:

    col1, col2, col3, col4 = st.columns(4)

    with col1:

        st.write("**Student**")

        st.write(
            latest_event.get("student_id")
        )

    with col2:

        st.write("**Book**")

        st.write(
            latest_event.get("book_name")
        )

    with col3:

        st.write("**Action**")

        action = latest_event.get("action")

        if action == "BORROW":

            st.success("📖 BORROW")

        else:

            st.info("↩️ RETURN")

    with col4:

        st.write("**Time**")

        st.write(
            latest_event.get("timestamp")
        )


else:

    st.info(
        "No transaction matches the selected filters."
    )


st.divider()


# ============================================================
# RECENT TRANSACTIONS
# ============================================================

st.subheader("📋 Recent Library Transactions")


if events:

    recent_events = events[-10:]

    for event in reversed(recent_events):

        action = event.get("action")

        if action == "BORROW":

            icon = "📖"

        else:

            icon = "↩️"

        st.write(
            f"{icon} "
            f"**{event.get('student_id')}** → "
            f"**{event.get('book_name')}** → "
            f"**{action}** → "
            f"{event.get('timestamp')}"
        )


else:

    st.info(
        "No transactions match the selected filters."
    )


# ============================================================
# AUTO REFRESH
# ============================================================

time.sleep(2)

st.rerun()
