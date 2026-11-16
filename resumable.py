"""Budgeted, JSON-round-trippable SDK1 evaluation.

The evaluator state is an explicit continuation stack made only of JSON data:
the validated plan, the bound root environment, and one frame per expression
currently being evaluated.  Frames record which child is awaited and any
already-computed operands, so a pause may occur inside an operand, a nested
source, an initial value, or a body, including several same-named scopes.

A work unit is exactly one dynamic expression entry.  ``progress_probe.entered``
is called immediately before each such entry; restoring a frame, scanning the
checkpoint and structure validation never call it.
"""
import copy

import progress_probe
import sdk1
from values import WarehouseError, array, clone, fail, plus, truth

VERSION = 1


class _HookError(BaseException):
    """Carries an observer exception past the business-error handler."""

    def __init__(self, original):
        self.original = original


def validate_budget(budget):
    """Reject non-boolean positive integers without touching any state."""
    if type(budget) is not int or budget <= 0:
        raise ValueError("budget must be a positive non-boolean integer")


def start(plan, parameters):
    """Validate and snapshot, but enter no expression."""
    sdk1.validate(plan)
    document = copy.deepcopy(plan)
    environment = sdk1.bind(plan, parameters)
    return {
        "version": VERSION,
        "plan": document,
        "stack": [_frame(document["body"], environment)],
        "pending": None,
        "state": "queued",
        "output": None,
        "error": None,
    }


def advance(checkpoint, budget):
    """Drive the continuation for at most ``budget`` new expression entries."""
    validate_budget(budget)
    if checkpoint["state"] != "queued":
        return {
            "checkpoint": copy.deepcopy(checkpoint),
            "state": checkpoint["state"],
            "used": 0,
            "output": copy.deepcopy(checkpoint["output"]),
            "error": copy.deepcopy(checkpoint["error"]),
        }
    work = copy.deepcopy(checkpoint)
    counter = [0]
    try:
        _drive(work, budget, counter)
    except _HookError as wrapper:
        raise wrapper.original
    except WarehouseError as error:
        work["state"] = "failed"
        work["stack"] = []
        work["pending"] = None
        work["output"] = None
        work["error"] = {"code": error.code, "message": error.message}
    return {
        "checkpoint": work,
        "state": work["state"],
        "used": counter[0],
        "output": copy.deepcopy(work["output"]),
        "error": copy.deepcopy(work["error"]),
    }


def _frame(node, environment):
    return {"node": node, "environment": environment,
            "entered": False, "phase": "start"}


def _layer(environment, names, values):
    local = dict(environment)
    for key, value in zip(names, values):
        local[key] = value
    return local


def _complete(stack, work, value):
    stack.pop()
    work["pending"] = {"value": value}


def _drive(work, budget, counter):
    stack = work["stack"]
    while True:
        pending = work["pending"]
        if pending is not None:
            work["pending"] = None
            if not stack:
                work["state"] = "done"
                work["output"] = pending["value"]
                return
            _resume(stack[-1], pending["value"], stack, work)
            continue
        if not stack:
            raise AssertionError("queued checkpoint has no pending work")
        frame = stack[-1]
        if not frame["entered"]:
            if counter[0] >= budget:
                return
            counter[0] += 1
            try:
                progress_probe.entered(frame["node"])
            except BaseException as error:
                raise _HookError(error)
            frame["entered"] = True
            _begin(frame, stack, work)


def _begin(frame, stack, work):
    """Begin an entered frame: evaluate its first child or complete it."""
    node = frame["node"]
    op = node["op"]
    environment = frame["environment"]
    if op == "const":
        _complete(stack, work, clone(node["value"]))
    elif op == "var":
        key = node["name"]
        if key not in environment:
            fail("unbound", key)
        _complete(stack, work, environment[key])
    elif op == "add":
        frame["phase"] = "left"
        stack.append(_frame(node["left"], environment))
    elif op == "if":
        frame["phase"] = "test"
        stack.append(_frame(node["test"], environment))
    elif op == "map":
        frame["phase"] = "source"
        stack.append(_frame(node["array"], environment))
    else:
        frame["phase"] = "source"
        stack.append(_frame(node["array"], environment))


def _resume(frame, value, stack, work):
    """Deliver a completed child to its waiting frame."""
    node = frame["node"]
    op = node["op"]
    environment = frame["environment"]
    phase = frame["phase"]
    if op == "add":
        if phase == "left":
            frame["left"] = value
            frame["phase"] = "right"
            stack.append(_frame(node["right"], environment))
        else:
            _complete(stack, work, plus(frame["left"], value))
    elif op == "if":
        if phase == "test":
            branch = "then" if truth(value) else "else"
            frame["branch"] = branch
            frame["phase"] = "branch"
            stack.append(_frame(node[branch], environment))
        else:
            _complete(stack, work, value)
    elif op == "map":
        if phase == "source":
            source = array(value)
            if source is None:
                _complete(stack, work, None)
            elif not source:
                _complete(stack, work, [])
            else:
                frame["source"] = source
                frame["index"] = 0
                frame["result"] = []
                frame["phase"] = "body"
                stack.append(_frame(
                    node["body"],
                    _layer(environment, (node["param"],), (source[0],))))
        else:
            frame["result"].append(value)
            index = frame["index"] + 1
            frame["index"] = index
            if index < len(frame["source"]):
                stack.append(_frame(
                    node["body"],
                    _layer(environment, (node["param"],), (frame["source"][index],))))
            else:
                _complete(stack, work, frame["result"])
    else:
        if phase == "source":
            source = array(value)
            if source is None:
                _complete(stack, work, None)
            else:
                frame["source"] = source
                frame["phase"] = "initial"
                stack.append(_frame(node["initial"], environment))
        elif phase == "initial":
            frame["acc"] = value
            frame["index"] = 0
            if not frame["source"]:
                _complete(stack, work, value)
            else:
                frame["phase"] = "body"
                stack.append(_frame(
                    node["body"],
                    _layer(environment, (node["acc"], node["item"]),
                           (value, frame["source"][0]))))
        else:
            frame["acc"] = value
            index = frame["index"] + 1
            frame["index"] = index
            if index < len(frame["source"]):
                stack.append(_frame(
                    node["body"],
                    _layer(environment, (node["acc"], node["item"]),
                           (value, frame["source"][index]))))
            else:
                _complete(stack, work, value)
