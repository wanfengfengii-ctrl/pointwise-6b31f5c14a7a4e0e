"""Public examples and checks for the new capability; contains no evaluator."""
import argparse
import copy
import importlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from catalog import Catalog
from jobs import Jobs
from storage import Database
from samples import A, C, F, I, M, P, V, create_frozen, nested
from values import WarehouseError
from progress_probe import capture, reference, fail_before_commit, CommitFailure


def cases():
    return [
        (P([], A(C(1), C(2))), {}),
        (nested(), {"rows": [[10, 20], [], [None, 10]], "base": 7}),
        (P([], F(M(C([1, 0]), "x", I(V("x"), C(5), V("source_error"))),
                  V("seed_error"), "a", "x", V("body_error"))), {}),
        (P([], F(C(None), V("seed"), "a", "x", V("body"))), {}),
        (P([], F(C([]), C([1]), "a", "x", V("body"))), {}),
        (P([], A(C([]), V("right_error"))), {}),
        (P([], M(C(9), "x", V("x"))), {}),
        (P([], I(C([]), C(1), C(2))), {}),
        (P([], I(C(0), V("unused"), C(None))), {}),
        (P(["x"], A(F(C([10, 20]), C(0), "a", "x",
                       A(V("a"), F(C([2, 3]), V("x"), "a", "x",
                                      A(V("a"), V("x"))))), V("x"))),
         {"x": 100}),
    ]


def roundtrip(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def check_runtime():
    runtime = importlib.import_module("resumable")
    for plan, parameters in cases():
        expected = reference(plan, parameters)
        for budget in (1, 2, 5):
            with capture() as trace:
                checkpoint = runtime.start(plan, parameters)
            assert trace == []
            checkpoint = roundtrip(checkpoint)
            snapshot = copy.deepcopy(checkpoint)
            for bad in (True, 0, -1, 1.5, None):
                with capture() as trace:
                    try:
                        runtime.advance(checkpoint, bad)
                    except ValueError:
                        pass
                    else:
                        raise AssertionError("invalid budget accepted")
                assert checkpoint == snapshot and trace == []
            total, sequence = 0, []
            for _ in range(len(expected["trace"]) + 2):
                with capture() as trace:
                    result = runtime.advance(checkpoint, budget)
                assert type(result["used"]) is int
                assert 0 <= result["used"] <= budget
                assert result["used"] == len(trace)
                sequence.extend(trace)
                total += result["used"]
                checkpoint = roundtrip(result["checkpoint"])
                if result["state"] == "queued":
                    assert result["used"] == budget
                    assert result["output"] is None and result["error"] is None
                    continue
                assert result["state"] == expected["state"]
                assert result["output"] == expected["output"]
                assert result["error"] == expected["error"]
                break
            else:
                raise AssertionError("runtime failed to finish")
            assert sequence == expected["trace"] and total == len(sequence)
            with capture() as trace:
                terminal = runtime.advance(checkpoint, 1)
            assert terminal["state"] == result["state"]
            assert terminal["used"] == 0 and trace == []
            assert terminal["output"] == result["output"]
            assert terminal["error"] == result["error"]
    return len(cases())


def outcome(jobs, job):
    try:
        return {"output": jobs.result(job), "error": None}
    except WarehouseError as error:
        return {"output": None, "error": {"code": error.code, "message": error.message}}


def check_jobs():
    reports = []
    with TemporaryDirectory(prefix="resume-check-") as directory:
        for number, (plan, parameters) in enumerate(cases()):
            expected = reference(plan, parameters)
            path = Path(directory) / (str(number) + ".sqlite")
            supplied = copy.deepcopy(parameters)
            with Database(path) as db:
                catalog, jobs = Catalog(db), Jobs(db)
                catalog.save("p", plan)
                job = jobs.enqueue("p", supplied)
                supplied.clear()
                catalog.save("p", P([], C(999)))
            total, sequence = 0, []
            for _ in range(len(expected["trace"]) + 2):
                with Database(path) as db:
                    jobs = Jobs(db)
                    with capture() as trace:
                        step = jobs.step(job, 1)
                    assert step["used"] == len(trace) == 1
                    total += step["used"]
                    sequence.extend(trace)
                    assert step["total"] == total
                    assert jobs.status(job)["revision"] == 1
                    if step["state"] == "queued":
                        assert outcome(jobs, job)["error"]["code"] == "pending"
                        continue
                    assert step["state"] == expected["state"]
                    assert outcome(jobs, job) == {
                        "output": expected["output"], "error": expected["error"]}
                break
            else:
                raise AssertionError("persistent task failed to finish")
            assert sequence == expected["trace"]
            with Database(path) as db:
                jobs = Jobs(db)
                with capture() as trace:
                    again = jobs.step(job, 2)
                    assert jobs.run(job) == expected["state"]
                assert again == {"state": expected["state"], "used": 0, "total": total}
                assert trace == []
            reports.append({"case": number, "state": step["state"], "total": total})
        path = Path(directory) / "rollback.sqlite"
        with Database(path) as db:
            Catalog(db).save("p", P([], A(C(1), C(2))))
            job = Jobs(db).enqueue("p", {})
            assert Jobs(db).step(job, 1) == {"state": "queued", "used": 1, "total": 1}
        with Database(path) as db:
            assert Jobs(db).step(job, 1) == {"state": "queued", "used": 1, "total": 2}
            before = list(db.connection.iterdump())
            try:
                with fail_before_commit(db):
                    Jobs(db).step(job, 1)
            except CommitFailure:
                pass
            else:
                raise AssertionError("step bypassed the transaction fault seam")
            assert list(db.connection.iterdump()) == before
        with Database(path) as db:
            jobs = Jobs(db)
            assert jobs.status(job)["state"] == "queued"
            assert outcome(jobs, job)["error"]["code"] == "pending"
            with capture() as trace:
                step = jobs.step(job, 1)
            assert step == {"state": "done", "used": 1, "total": 3}
            assert trace == [C(2)] and jobs.result(job) == 3
        path = Path(directory) / "legacy.sqlite"
        create_frozen(path)
        with Database(path) as db:
            jobs = Jobs(db)
            for job, state in ((3, "done"), (4, "failed"), (6, "done")):
                assert jobs.step(job, 1) == {"state": state, "used": 0, "total": 0}
            assert jobs.step(2, 1)["state"] == "queued"
        with Database(path) as db:
            jobs = Jobs(db)
            with capture() as trace:
                assert jobs.run(2) == "done"
            expected = reference(nested(), {"rows": [[10, 20], [], [None, 10]], "base": 7})
            assert trace == expected["trace"][1:]
            assert jobs.result(2) == expected["output"]
            assert jobs.step(2, 1) == {
                "state": "done", "used": 0, "total": len(expected["trace"])}
    return reports


def demo():
    with TemporaryDirectory(prefix="resume-demo-") as directory:
        path = Path(directory) / "demo.sqlite"
        plan = nested()
        args = {"rows": [[10, 20], [], [None, 10]], "base": 7}
        expected = reference(plan, args)
        with Database(path) as db:
            Catalog(db).save("daily", plan)
            job = Jobs(db).enqueue("daily", args)
            first = Jobs(db).step(job, 7)
            assert first == {"state": "queued", "used": 7, "total": 7}
            assert outcome(Jobs(db), job)["error"]["code"] == "pending"
            Catalog(db).save("daily", P([], C(999)))
        with Database(path) as db:
            jobs = Jobs(db)
            with capture() as trace:
                assert jobs.run(job) == "done"
            assert trace == expected["trace"][7:]
            assert jobs.result(job) == expected["output"]
            final = jobs.step(job, 1)
            assert final["total"] == len(expected["trace"])
            return {"before_close": first, "after_reopen": final,
                    "pinned_revision": jobs.status(job)["revision"],
                    "current_head": Catalog(db).revision("daily"),
                    "result": jobs.result(job), "resumed_entries": len(trace)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("verify", "demo"))
    command = parser.parse_args().command
    if command == "verify":
        import unittest
        suite = unittest.defaultTestLoader.discover("tests")
        original = unittest.TextTestRunner(verbosity=2).run(suite)
        if not original.wasSuccessful():
            raise SystemExit(1)
        report = {"runtime_cases": check_runtime(), "reopened_jobs": check_jobs()}
    else:
        report = demo()
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
