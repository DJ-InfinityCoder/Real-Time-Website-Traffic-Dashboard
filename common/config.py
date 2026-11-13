"""Central configuration. Every value can be overridden with an environment variable."""
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "web-logs")

# HBase is accessed through its Thrift1 gateway (port 9090) using happybase.
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8050"))

SPARK_TRIGGER_SECONDS = int(os.getenv("SPARK_TRIGGER_SECONDS", "2"))
CHECKPOINT_DIR = os.getenv(
    "CHECKPOINT_DIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "checkpoints", "web-logs"),
)
