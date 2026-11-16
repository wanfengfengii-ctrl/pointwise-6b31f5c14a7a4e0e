"""Durable revision/parameter snapshots and resumable single-worker execution."""
import json
import resumable
import sdk1
from catalog import lookup
from values import WarehouseError, dumps, fail

RUNTIME_TABLE = "job_checkpoints_v1"

_RUNTIME_DDL = """
CREATE TABLE IF NOT EXISTS job_checkpoints_v1 (
    job_id INTEGER PRIMARY KEY,
    checkpoint TEXT NOT NULL,
    total INTEGER NOT NULL CHECK(total>=0),
    FOREIGN KEY(job_id) REFERENCES jobs_v1(id)
)
"""


def job_row(connection, job_id):
    row = connection.execute("SELECT * FROM jobs_v1 WHERE id=?", (job_id,)).fetchone()
    if row is None:
        fail("not_found", "job does not exist")
    return row


def _runtime_exists(connection):
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (RUNTIME_TABLE,)
    ).fetchone() is not None


def _runtime_row(connection, job_id):
    return connection.execute(
        "SELECT checkpoint,total FROM " + RUNTIME_TABLE + " WHERE job_id=?", (job_id,)
    ).fetchone()


def _ensure_runtime(connection):
    connection.execute(_RUNTIME_DDL)


def _save_runtime(connection, job_id, checkpoint, total):
    connection.execute(
        "INSERT INTO " + RUNTIME_TABLE + " (job_id,checkpoint,total) VALUES (?,?,?) "
        "ON CONFLICT(job_id) DO UPDATE SET checkpoint=excluded.checkpoint,"
        "total=excluded.total",
        (job_id, dumps(checkpoint), total),
    )


def _stored_total(connection, job_id):
    """A terminal task keeps its accumulated total; a historical one starts at zero."""
    if not _runtime_exists(connection):
        return 0
    row = _runtime_row(connection, job_id)
    return 0 if row is None else row["total"]


def _load(connection, job):
    """Return (checkpoint, total); a never-stepped queued job starts fresh."""
    row = _runtime_row(connection, job["id"])
    if row is None:
        plan = lookup(connection, job["name"], job["revision"])
        checkpoint = resumable.start(json.loads(plan["payload"]),
                                     json.loads(job["parameters"]))
        return checkpoint, 0
    return json.loads(row["checkpoint"]), row["total"]


def _publish(connection, job_id, checkpoint, total):
    if checkpoint["state"] == "done":
        connection.execute(
            "UPDATE jobs_v1 SET state='done',output=?,error=NULL WHERE id=?",
            (dumps(checkpoint["output"]), job_id),
        )
    elif checkpoint["state"] == "failed":
        connection.execute(
            "UPDATE jobs_v1 SET state='failed',output=NULL,error=? WHERE id=?",
            (dumps(checkpoint["error"]), job_id),
        )
    _save_runtime(connection, job_id, checkpoint, total)


class Jobs:
    def __init__(self, database):
        self.database = database

    def enqueue(self, name, parameters):
        with self.database.transaction() as connection:
            self.database.require_v1()
            plan = lookup(connection, name)
            snapshot = sdk1.bind(json.loads(plan["payload"]), parameters)
            cursor = connection.execute(
                "INSERT INTO jobs_v1(name,revision,parameters,state) VALUES (?,?,?,'queued')",
                (name, plan["revision"], dumps(snapshot)),
            )
            return cursor.lastrowid

    def run(self, job_id):
        with self.database.transaction() as connection:
            self.database.require_v1()
            job = job_row(connection, job_id)
            if job["state"] != "queued":
                return job["state"]
            _ensure_runtime(connection)
            checkpoint, total = _load(connection, job)
            while checkpoint["state"] == "queued":
                outcome = resumable.advance(checkpoint, 1 << 62)
                total += outcome["used"]
                checkpoint = outcome["checkpoint"]
            _publish(connection, job_id, checkpoint, total)
            return checkpoint["state"]

    def step(self, job_id, budget):
        resumable.validate_budget(budget)
        with self.database.transaction() as connection:
            self.database.require_v1()
            job = job_row(connection, job_id)
            if job["state"] != "queued":
                return {"state": job["state"], "used": 0,
                        "total": _stored_total(connection, job_id)}
            _ensure_runtime(connection)
            checkpoint, total = _load(connection, job)
            outcome = resumable.advance(checkpoint, budget)
            total += outcome["used"]
            _publish(connection, job_id, outcome["checkpoint"], total)
            return {"state": outcome["state"], "used": outcome["used"], "total": total}

    def status(self, job_id):
        self.database.require_v1()
        row = job_row(self.database.connection, job_id)
        return {"id": row["id"], "name": row["name"], "revision": row["revision"],
                "state": row["state"]}

    def result(self, job_id):
        self.database.require_v1()
        row = job_row(self.database.connection, job_id)
        if row["state"] == "queued":
            fail("pending", "job has not completed")
        if row["state"] == "failed":
            error = json.loads(row["error"])
            raise WarehouseError(error["code"], error["message"])
        return json.loads(row["output"])
