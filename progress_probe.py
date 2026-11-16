"""Observations only; no resumable evaluator or checkpoint implementation."""
import copy
from contextlib import contextmanager
from contextvars import ContextVar
from unittest.mock import patch
import sdk1
from values import WarehouseError

_trace = ContextVar("expression_trace", default=None)


def entered(node):
    """Call once immediately before each dynamic SDK1 expression entry."""
    trace = _trace.get()
    if trace is not None:
        trace.append(copy.deepcopy(node))


@contextmanager
def capture():
    trace = []
    token = _trace.set(trace)
    try:
        yield trace
    finally:
        _trace.reset(token)


def reference(plan, parameters):
    """Observe the unchanged recursive oracle, including its first error."""
    trace = []
    original = sdk1.evaluate

    def visit(node, env):
        trace.append(copy.deepcopy(node))
        return original(node, env)

    with patch.object(sdk1, "evaluate", side_effect=visit):
        try:
            output = sdk1.execute(plan, parameters)
        except WarehouseError as error:
            return {"state": "failed", "output": None,
                    "error": {"code": error.code, "message": error.message},
                    "trace": trace}
    return {"state": "done", "output": output, "error": None, "trace": trace}


class CommitFailure(RuntimeError):
    pass


@contextmanager
def fail_before_commit(database):
    """Use the existing transaction seam; preserve any caller commit hook."""
    original = database.transaction

    def transaction(before_commit=None):
        def stop(connection):
            if before_commit is not None:
                before_commit(connection)
            raise CommitFailure("injected before commit")
        return original(before_commit=stop)

    with patch.object(database, "transaction", side_effect=transaction):
        yield


def selfcheck():
    from samples import A, C, P, V, create_frozen
    from storage import Database
    plan = P([], A(C(1), C(2)))
    result = reference(plan, {})
    assert result["output"] == 3 and len(result["trace"]) == 3
    error = reference(P([], A(C([]), V("right"))), {})
    assert error["error"] == {"code": "unbound", "message": "right"}
    assert len(error["trace"]) == 3
    node = C([1])
    with capture() as outer:
        entered(node)
        node["value"][0] = 9
        with capture() as inner:
            entered(C(2))
        entered(C(3))
    assert outer == [C([1]), C(3)] and inner == [C(2)]
    with Database() as db:
        try:
            with fail_before_commit(db):
                with db.transaction() as connection:
                    connection.execute("CREATE TABLE probe (x INTEGER)")
                    connection.execute("INSERT INTO probe VALUES (7)")
        except CommitFailure:
            pass
        else:
            raise AssertionError("injection did not run")
        assert db.connection.execute(
            "SELECT name FROM sqlite_master WHERE name='probe'").fetchone() is None
        with db.transaction() as connection:
            connection.execute("CREATE TABLE probe (x INTEGER)")
        assert db.connection.execute(
            "SELECT name FROM sqlite_master WHERE name='probe'").fetchone() is not None
    create_frozen(":memory:")
    return {"trace_nodes": len(result["trace"]), "result": result["output"],
            "error_order": error["error"], "rollback": True,
            "frozen_memory_constructor": True}


if __name__ == "__main__":
    import json
    print(json.dumps(selfcheck(), ensure_ascii=False, sort_keys=True))
