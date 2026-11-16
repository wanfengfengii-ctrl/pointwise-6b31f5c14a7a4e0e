import copy
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import progress_probe
import resumable
from catalog import Catalog
from jobs import Jobs
from resume_checks import cases, outcome
from samples import A, C, F, I, M, P, V, create_frozen, nested
from storage import Database
from values import WarehouseError
from progress_probe import capture, reference, fail_before_commit, CommitFailure


def roundtrip(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


class RuntimeTests(unittest.TestCase):
    def test_randomized_differential_against_oracle(self):
        import random
        generator = random.Random(20261116)
        scalars = [None, 0, 1, 2, -3]
        names = ["x", "y", "a", "missing"]

        def make(depth):
            choice = generator.randrange(9 if depth > 0 else 3)
            if choice == 0:
                return C(generator.choice(scalars))
            if choice == 1:
                return C([generator.choice(scalars)
                          for _ in range(generator.randrange(4))])
            if choice == 2:
                return V(generator.choice(names))
            if choice == 3:
                return A(make(depth - 1), make(depth - 1))
            if choice == 4:
                return I(make(depth - 1), make(depth - 1), make(depth - 1))
            if choice == 5:
                param = generator.choice(["x", "y", "row"])
                return M(make(depth - 1), param, make(depth - 1))
            if choice == 6:
                return F(make(depth - 1), make(depth - 1), "a", "x",
                         make(depth - 1))
            if choice == 7:
                return F(C(generator.choice([[1, 2], [], None, [0, 1]])),
                         make(depth - 1), "y", "x", make(depth - 1))
            return M(C([[1], None, [2, 3], []]), "x", make(depth - 1))

        plans = []
        for _ in range(40):
            parameters = {}
            if generator.random() < 0.5:
                parameters["x"] = generator.choice(scalars + [[1, 2], []])
            if generator.random() < 0.5:
                parameters["y"] = generator.choice(scalars)
            plans.append(P(sorted(parameters), make(3)))
            plans[-1] = (plans[-1], parameters)
        for plan, parameters in plans:
            expected = reference(plan, parameters)
            for budget in (1, 2, 4):
                checkpoint = roundtrip(resumable.start(plan, parameters))
                sequence, total = [], 0
                for _ in range(len(expected["trace"]) + 3):
                    with capture() as trace:
                        result = resumable.advance(checkpoint, budget)
                    self.assertEqual(result["used"], len(trace))
                    sequence.extend(trace)
                    total += result["used"]
                    checkpoint = roundtrip(result["checkpoint"])
                    if result["state"] != "queued":
                        break
                with self.subTest(plan=plan, parameters=parameters, budget=budget):
                    self.assertEqual(result["state"], expected["state"])
                    self.assertEqual(result["output"], expected["output"])
                    self.assertEqual(result["error"], expected["error"])
                    self.assertEqual(sequence, expected["trace"])
                    self.assertEqual(total, len(expected["trace"]))

    def test_hook_errors_are_not_business_failures(self):
        plan = P([], A(C(1), C(2)))
        checkpoint = resumable.start(plan, {})
        for original in (RuntimeError("observer"), WarehouseError("unbound", "observer")):
            with patch.object(progress_probe, "entered", side_effect=original):
                with self.assertRaises(type(original)) as caught:
                    resumable.advance(checkpoint, 1)
            self.assertIs(caught.exception, original)
        self.assertEqual(checkpoint["state"], "queued")
        with TemporaryDirectory(prefix="hook-test-") as directory:
            with Database(Path(directory) / "hook.sqlite") as db:
                Catalog(db).save("p", plan)
                job = Jobs(db).enqueue("p", {})
                before = list(db.connection.iterdump())
                with patch.object(progress_probe, "entered",
                                  side_effect=WarehouseError("unbound", "observer")):
                    with self.assertRaises(WarehouseError) as caught:
                        Jobs(db).step(job, 2)
                self.assertEqual((caught.exception.code, caught.exception.message),
                                 ("unbound", "observer"))
                self.assertEqual(list(db.connection.iterdump()), before)
                self.assertEqual(Jobs(db).status(job)["state"], "queued")

    def test_exact_budget_finishes_without_extra_poll(self):
        plan = P([], A(C(1), C(2)))
        with capture() as trace:
            result = resumable.advance(resumable.start(plan, {}), 3)
        self.assertEqual((result["state"], result["used"], result["output"]),
                         ("done", 3, 3))
        self.assertEqual(len(trace), 3)
        single = P([], C(7))
        with capture() as trace:
            result = resumable.advance(resumable.start(single, {}), 1)
        self.assertEqual((result["state"], result["used"], result["output"]),
                         ("done", 1, 7))
        self.assertEqual(len(trace), 1)

    def test_checkpoint_roundtrip_into_fresh_process_state(self):
        plan = nested()
        arguments = {"rows": [[10, 20], [None]], "base": 7}
        expected = reference(plan, arguments)
        checkpoint = roundtrip(resumable.start(plan, arguments))
        for budget in (4, 9):
            result = resumable.advance(checkpoint, budget)
            self.assertEqual(result["checkpoint"]["state"], "queued")
            checkpoint = roundtrip(result["checkpoint"])
        resumed = roundtrip(checkpoint)
        sequence, total = [], 0
        while True:
            with capture() as trace:
                result = resumable.advance(resumed, 1)
            sequence.extend(trace)
            total += result["used"]
            resumed = roundtrip(result["checkpoint"])
            if result["state"] != "queued":
                break
        self.assertEqual(result["output"], expected["output"])
        self.assertEqual(sequence, expected["trace"][13:])
        self.assertEqual(total, len(expected["trace"]) - 13)

    def test_advance_does_not_alias_input_checkpoint(self):
        plan = P(["rows"], M(V("rows"), "x", V("x")))
        checkpoint = resumable.start(plan, {"rows": [[1], [2]]})
        snapshot = copy.deepcopy(checkpoint)
        resumable.advance(checkpoint, 2)
        self.assertEqual(checkpoint, snapshot)
        first = resumable.advance(checkpoint, 100)
        self.assertEqual(first["output"], [[1], [2]])
        first["output"][0][0] = 9
        self.assertEqual(checkpoint, snapshot)

    def test_trace_matches_oracle_for_every_budget(self):
        for plan, parameters in cases():
            expected = reference(plan, parameters)
            for budget in (1, 2, 3, 7):
                with self.subTest(body=plan["body"], budget=budget):
                    checkpoint = roundtrip(resumable.start(plan, parameters))
                    sequence, total = [], 0
                    for _ in range(len(expected["trace"]) + 2):
                        with capture() as trace:
                            result = resumable.advance(checkpoint, budget)
                        self.assertEqual(result["used"], len(trace))
                        self.assertLessEqual(result["used"], budget)
                        sequence.extend(trace)
                        total += result["used"]
                        checkpoint = roundtrip(result["checkpoint"])
                        if result["state"] == "queued":
                            self.assertEqual(result["used"], budget)
                            self.assertIsNone(result["output"])
                            self.assertIsNone(result["error"])
                        else:
                            break
                    self.assertEqual(result["state"], expected["state"])
                    self.assertEqual(result["output"], expected["output"])
                    self.assertEqual(result["error"], expected["error"])
                    self.assertEqual(sequence, expected["trace"])
                    self.assertEqual(total, len(expected["trace"]))
                    with capture() as trace:
                        terminal = resumable.advance(checkpoint, budget)
                    self.assertEqual(trace, [])
                    self.assertEqual(terminal["used"], 0)
                    self.assertEqual(terminal["state"], expected["state"])
                    self.assertEqual(terminal["output"], expected["output"])
                    self.assertEqual(terminal["error"], expected["error"])
                    with self.assertRaises(ValueError):
                        resumable.advance(checkpoint, 0)

    def test_start_is_inert_and_snapshots_inputs(self):
        plan = P(["rows", "base"], M(V("rows"), "x", A(V("x"), V("base"))))
        arguments = {"rows": [1, 2], "base": 7}
        with capture() as trace:
            checkpoint = resumable.start(plan, arguments)
        self.assertEqual(trace, [])
        plan["body"] = C(999)
        arguments["rows"][0] = 1000
        arguments["base"] = -1
        checkpoint = roundtrip(checkpoint)
        used = 0
        for _ in range(100):
            outcome_ = resumable.advance(checkpoint, 1)
            used += outcome_["used"]
            checkpoint = outcome_["checkpoint"]
            if outcome_["state"] != "queued":
                break
        self.assertEqual(outcome_["output"], [8, 9])
        self.assertEqual(used, 8)

    def test_start_rejects_bad_inputs_without_entering(self):
        for plan, parameters, code in (
            (P([], C(1)), {"extra": 1}, "invalid_params"),
            (P([], {"op": "unknown"}), {}, "invalid_plan"),
        ):
            with capture() as trace:
                with self.assertRaises(WarehouseError) as caught:
                    resumable.start(plan, parameters)
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(trace, [])

    def test_pause_inside_operands_and_first_error(self):
        plan = P([], A(C([]), V("right")))
        checkpoint = resumable.start(plan, {})
        first = resumable.advance(checkpoint, 1)
        self.assertEqual((first["state"], first["used"]), ("queued", 1))
        checkpoint = roundtrip(first["checkpoint"])
        second = resumable.advance(checkpoint, 1)
        self.assertEqual((second["state"], second["used"]), ("queued", 1))
        checkpoint = roundtrip(second["checkpoint"])
        with capture() as trace:
            third = resumable.advance(checkpoint, 1)
        self.assertEqual([node["op"] for node in trace], ["var"])
        self.assertEqual(third["state"], "failed")
        self.assertEqual(third["error"], {"code": "unbound", "message": "right"})
        self.assertEqual(third["used"], 1)

    def test_fold_null_skips_initial_and_body(self):
        plan = P([], F(C(None), V("seed_error"), "a", "x", V("body_error")))
        checkpoint = resumable.start(plan, {})
        with capture() as trace:
            first = resumable.advance(checkpoint, 1)
        self.assertEqual([node["op"] for node in trace], ["fold"])
        self.assertEqual((first["state"], first["used"]), ("queued", 1))
        with capture() as trace:
            result = resumable.advance(roundtrip(first["checkpoint"]), 1)
        self.assertEqual([node["op"] for node in trace], ["const"])
        self.assertEqual((result["state"], result["output"], result["used"]),
                         ("done", None, 1))

    def test_no_progress_past_error_as_side_effects(self):
        plan = P([], I(C(0), V("unused"), C(None)))
        checkpoint = resumable.start(plan, {})
        with capture() as trace:
            result = resumable.advance(checkpoint, 100)
        self.assertEqual([node["op"] for node in trace], ["if", "const", "const"])
        self.assertEqual((result["state"], result["output"]), ("done", None))


class PersistentStepTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory(prefix="resumable-test-")
        self.path = Path(self.directory.name) / "warehouse.sqlite"

    def tearDown(self):
        self.directory.cleanup()

    def test_bad_budget_raises_before_any_change(self):
        with Database(self.path) as db:
            Catalog(db).save("p", P([], A(C(1), C(2))))
            jobs = Jobs(db)
            job = jobs.enqueue("p", {})
            before = list(db.connection.iterdump())
            for budget in (True, False, 0, -1, 1.5, "1", None):
                with capture() as trace:
                    with self.assertRaises(ValueError):
                        jobs.step(job, budget)
                self.assertEqual(trace, [])
                self.assertEqual(list(db.connection.iterdump()), before)
            with self.assertRaises(ValueError):
                jobs.step(424242, 0)
            self.assertEqual(jobs.status(job)["state"], "queued")
            jobs.step(job, 3)
            before = list(db.connection.iterdump())
            with capture() as trace:
                for budget in (True, 0, -2, 2.0, None):
                    with self.assertRaises(ValueError):
                        jobs.step(job, budget)
            self.assertEqual(trace, [])
            self.assertEqual(list(db.connection.iterdump()), before)
            self.assertEqual(jobs.status(job)["state"], "done")

    def test_business_failure_is_committed_with_error_and_total(self):
        with Database(self.path) as db:
            Catalog(db).save("p", P([], A(C([]), V("right"))))
            job = Jobs(db).enqueue("p", {})
            self.assertEqual(Jobs(db).step(job, 2),
                             {"state": "queued", "used": 2, "total": 2})
        with Database(self.path) as db:
            jobs = Jobs(db)
            self.assertEqual(jobs.step(job, 5),
                             {"state": "failed", "used": 1, "total": 3})
            with self.assertRaises(WarehouseError) as caught:
                jobs.result(job)
            self.assertEqual((caught.exception.code, caught.exception.message),
                             ("unbound", "right"))
            self.assertEqual(jobs.step(job, 1),
                             {"state": "failed", "used": 0, "total": 3})
            self.assertEqual(jobs.status(job)["state"], "failed")

    def test_business_failure_rolls_back_entirely_before_commit(self):
        with Database(self.path) as db:
            Catalog(db).save("p", P([], A(C([]), V("right"))))
            job = Jobs(db).enqueue("p", {})
            Jobs(db).step(job, 2)
        with Database(self.path) as db:
            jobs = Jobs(db)
            before = list(db.connection.iterdump())
            with self.assertRaises(CommitFailure):
                with fail_before_commit(db):
                    jobs.step(job, 5)
            self.assertEqual(list(db.connection.iterdump()), before)
            self.assertEqual(jobs.status(job)["state"], "queued")
            self.assertEqual(outcome(jobs, job)["error"]["code"], "pending")
            self.assertEqual(jobs.step(job, 5),
                             {"state": "failed", "used": 1, "total": 3})
            with self.assertRaises(WarehouseError) as caught:
                jobs.result(job)
            self.assertEqual(caught.exception.code, "unbound")

    def test_non_business_failure_rolls_back_and_retries_only_new_work(self):
        with Database(self.path) as db:
            Catalog(db).save("p", nested())
            job = Jobs(db).enqueue(
                "p", {"rows": [[10, 20], [], [None, 10]], "base": 7})
            Jobs(db).step(job, 5)
        expected = reference(nested(),
                             {"rows": [[10, 20], [], [None, 10]], "base": 7})
        with Database(self.path) as db:
            jobs = Jobs(db)
            before = list(db.connection.iterdump())
            calls = []

            def explode(node):
                calls.append(node)
                raise RuntimeError("probe exploded")

            with patch.object(progress_probe, "entered", side_effect=explode):
                with self.assertRaisesRegex(RuntimeError, "probe exploded"):
                    jobs.step(job, 4)
            self.assertEqual(len(calls), 1)
            self.assertEqual(list(db.connection.iterdump()), before)
            self.assertEqual(jobs.step(job, 3),
                             {"state": "queued", "used": 3, "total": 8})
            with capture() as trace:
                self.assertEqual(jobs.run(job), "done")
            self.assertEqual(trace, expected["trace"][8:])
            self.assertEqual(jobs.result(job), expected["output"])
            self.assertEqual(jobs.step(job, 1)["total"], len(expected["trace"]))

    def test_partial_arrays_stay_hidden_and_head_does_not_rebind(self):
        with Database(self.path) as db:
            catalog, jobs = Catalog(db), Jobs(db)
            catalog.save("daily", nested())
            job = jobs.enqueue("daily", {"rows": [[10, 20]], "base": 7})
            for _ in range(12):
                step = jobs.step(job, 1)
                if step["state"] != "queued":
                    break
                with self.assertRaises(WarehouseError) as caught:
                    jobs.result(job)
                self.assertEqual(caught.exception.code, "pending")
                self.assertEqual(jobs.status(job)["revision"], 1)
            catalog.save("daily", P([], C(999)))
            self.assertEqual(catalog.revision("daily"), 2)
            self.assertEqual(jobs.status(job)["revision"], 1)
            self.assertEqual(jobs.run(job), "done")
            self.assertEqual(jobs.result(job), [[37, 57]])

    def test_frozen_queued_starts_fresh_and_terminal_keeps_zero_total(self):
        create_frozen(self.path)
        with Database(self.path) as db:
            jobs = Jobs(db)
            for job, state in ((3, "done"), (4, "failed"), (6, "done")):
                self.assertEqual(jobs.step(job, 9),
                                 {"state": state, "used": 0, "total": 0})
            self.assertEqual(jobs.step(1, 1),
                             {"state": "queued", "used": 1, "total": 1})
            with capture() as trace:
                self.assertEqual(jobs.run(1), "done")
            reference_result = reference(
                P(["rows", "base"], M(V("rows"), "x", A(V("x"), V("base")))),
                {"rows": [2, 2, None], "base": 7})
            self.assertEqual(trace, reference_result["trace"][1:])
            self.assertEqual(jobs.result(1), [9, 9, None])
            self.assertEqual(jobs.step(1, 1),
                             {"state": "done", "used": 0,
                              "total": len(reference_result["trace"])})
            self.assertEqual(jobs.step(3, 1), {"state": "done", "used": 0, "total": 0})


if __name__ == "__main__":
    unittest.main()
