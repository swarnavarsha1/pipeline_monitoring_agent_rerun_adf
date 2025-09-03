# monitoring_agent/context_store.py

import sqlite3
from datetime import datetime, timezone
import threading
import logging
from shared.config import RETRY_THRESHOLD

logger = logging.getLogger(__name__)

class ContextStoreSQLite:
    def __init__(self, db_path="context_store.db"):
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_run_table()
        self._create_metadata_table()

    def _create_run_table(self):
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                pipeline_id TEXT,
                retry_count INTEGER,
                status TEXT,
                last_updated TIMESTAMP
            )
            """)

    def _create_metadata_table(self):
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """)

    def create_or_update_run(self, run_id, pipeline_id, status, retry_count=None):
        if retry_count is None:
            retry_count = RETRY_THRESHOLD
        with self._lock, self.conn:
            self.conn.execute("""
            INSERT INTO pipeline_runs(run_id, pipeline_id, retry_count, status, last_updated)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                pipeline_id=excluded.pipeline_id,
                retry_count=excluded.retry_count,
                status=excluded.status,
                last_updated=excluded.last_updated
            """, (run_id, pipeline_id, retry_count, status, datetime.now(timezone.utc).isoformat()))

    def get_retry_count(self, run_id):
        with self._lock, self.conn:
            cursor = self.conn.execute("SELECT retry_count FROM pipeline_runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            return row[0] if row else RETRY_THRESHOLD

    def set_retry_count(self, run_id, count):
        with self._lock, self.conn:
            self.conn.execute("""
            UPDATE pipeline_runs SET retry_count = ?, last_updated = ?
            WHERE run_id = ?
            """, (count, datetime.now(timezone.utc).isoformat(), run_id))

    def update_status(self, run_id, status):
        with self._lock, self.conn:
            cur = self.conn.execute("""
            UPDATE pipeline_runs SET status = ?, last_updated = ?
            WHERE run_id = ?
            """, (status, datetime.now(timezone.utc).isoformat(), run_id))
            
            if cur.rowcount == 0:  # no rows updated → insert instead
                self.conn.execute("""
                INSERT INTO pipeline_runs (run_id, status, last_updated)
                VALUES (?, ?, ?)
                """, (run_id, status, datetime.now(timezone.utc).isoformat()))


    def get_last_query_time(self) -> datetime or None:
        with self._lock, self.conn:
            cursor = self.conn.execute("SELECT value FROM metadata WHERE key = ?", ("last_query_time",))
            row = cursor.fetchone()
            if row:
                return datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
            return None

    def set_last_query_time(self, dt: datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        with self._lock, self.conn:
            self.conn.execute("""
            INSERT INTO metadata (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, ("last_query_time", dt.isoformat()))

    def get_status(self, run_id):
        with self._lock, self.conn:
            cursor = self.conn.execute("SELECT status FROM pipeline_runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            return row[0] if row else None

