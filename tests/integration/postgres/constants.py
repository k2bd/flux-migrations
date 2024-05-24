import os

TEST_PG_CONNECTION_STRING = os.environ.get("TEST_PG_CONNECTION_STRING", "")

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

MIGRATIONS_1_DIR = os.path.join(DATA_DIR, "migrations-1")
