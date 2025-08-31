import os

DATABEND_DSL = os.environ.get(
    "DATABEND_DSN", "databend://root:root@localhost:8000/?sslmode=disable"
)
