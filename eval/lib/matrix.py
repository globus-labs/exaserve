"""Matrix expansion: combinatorial sweep over experiment axes.

Given an ExperimentSpec with a MatrixSpec (e.g., axes = [{name: num_nodes,
values: [1,2,4]}]), expand_matrix() produces one VariantSpec per point in
the Cartesian product. Each variant gets a deep-copied spec with the axis
values injected into the targeted dotted paths (e.g., "deployment.num_nodes").

Axes can target any field on the spec via dotted_set(). Fields *not* targeted
by the matrix (like client.num_nodes, scheduler.nodes) are auto-synced to
deployment.num_nodes so they stay consistent unless explicitly swept.

Derived fields (matrix.derived) are evaluated after all axis values are set
for each combination. Expressions can reference any axis name and use basic
math builtins (min, max, abs, round, int, float, plus the math module).
"""

from __future__ import annotations

import ast
import itertools
import math
from dataclasses import replace

from .models import ExperimentSpec, VariantSpec
from .utils import deep_copy, dotted_set, format_template, slugify

_DERIVED_FUNCS = {
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
    "int": int,
    "float": float,
}

# math.* is exposed as a read-only attribute namespace (math.ceil(x) etc.).
_ALLOWED_MATH = {
    name: getattr(math, name)
    for name in ("ceil", "floor", "sqrt", "log", "log2", "log10", "pow",
                 "exp", "pi", "e", "inf")
}

_ALLOWED_NODES = (
    ast.Expression, ast.Constant, ast.Name, ast.Load, ast.Call,
    ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare, ast.IfExp,
    ast.Attribute, ast.Tuple, ast.List,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
    ast.USub, ast.UAdd, ast.Not, ast.And, ast.Or,
    ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
)


def _eval_derived(expr: str, axis_values: dict) -> object:
    """AST-restricted expression evaluation (PR-016).

    A spec expression may use numeric/string literals, declared axis names,
    arithmetic/comparison/conditional operators, the approved functions
    (min/max/abs/round/int/float), and ``math.<approved>``. Everything else
    — attribute traversal, subscripts, lambdas, comprehensions, dunder
    access — is rejected. YAML specs are configuration, not code.
    """
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"matrix.derived expression invalid: {expr!r}: {exc}") from exc

    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ValueError(
                f"matrix.derived expression {expr!r} uses disallowed syntax "
                f"{type(node).__name__}")
        if isinstance(node, ast.Attribute):
            if not (isinstance(node.value, ast.Name) and node.value.id == "math"
                    and node.attr in _ALLOWED_MATH):
                raise ValueError(
                    f"matrix.derived expression {expr!r}: only math.<{'/'.join(sorted(_ALLOWED_MATH))}> attributes are allowed")
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise ValueError(f"matrix.derived expression {expr!r}: dunder names forbidden")
        if isinstance(node, ast.Call):
            func = node.func
            ok = (isinstance(func, ast.Name) and func.id in _DERIVED_FUNCS) or (
                isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
                and func.value.id == "math" and func.attr in _ALLOWED_MATH)
            if not ok:
                raise ValueError(
                    f"matrix.derived expression {expr!r}: only "
                    f"{sorted(_DERIVED_FUNCS)} and math.<fn> calls are allowed")

    class _MathNS:
        def __getattr__(self, name: str):
            try:
                return _ALLOWED_MATH[name]
            except KeyError:
                raise AttributeError(name) from None

    namespace: dict[str, object] = dict(_DERIVED_FUNCS)
    namespace["math"] = _MathNS()
    namespace.update(axis_values)
    code = compile(tree, "<matrix.derived>", "eval")
    return eval(code, {"__builtins__": {}}, namespace)  # noqa: S307 — AST-validated above


def expand_matrix(spec: ExperimentSpec) -> list[VariantSpec]:
    if not spec.matrix.axes:
        return [VariantSpec(spec=deep_copy(spec), variant_name="default", axis_values={})]

    axis_names = [axis.name for axis in spec.matrix.axes]
    axis_values = [axis.values for axis in spec.matrix.axes]

    combine = getattr(spec.matrix, "combine", "cartesian")
    if combine == "zip":
        lengths = [len(v) for v in axis_values]
        if len(set(lengths)) != 1:
            raise ValueError(
                f"matrix combine=zip requires all axes to have equal-length values lists, "
                f"got lengths {dict(zip(axis_names, lengths))}"
            )
        combinations = zip(*axis_values)
    else:
        combinations = itertools.product(*axis_values)

    variants = []
    for combination in combinations:
        values = dict(zip(axis_names, combination))
        spec_copy = deep_copy(spec)
        for axis in spec_copy.matrix.axes:
            value = values[axis.name]
            for target in axis.targets:
                dotted_set(spec_copy, target, value)

        for derived in spec.matrix.derived:
            derived_value = _eval_derived(derived.expr, values)
            dotted_set(spec_copy, derived.path, derived_value)

        targeted_fields = {t for axis in spec.matrix.axes for t in axis.targets}
        derived_fields = {d.path for d in spec.matrix.derived}
        if "client.num_nodes" not in targeted_fields and "client.num_nodes" not in derived_fields:
            spec_copy.client.num_nodes = spec_copy.deployment.num_nodes
        if "scheduler.nodes" not in targeted_fields and "scheduler.nodes" not in derived_fields:
            spec_copy.scheduler.nodes = spec_copy.deployment.num_nodes

        if spec.matrix.name_template:
            variant_name = format_template(spec.matrix.name_template, values)
        else:
            labels = []
            for axis in spec.matrix.axes:
                label = axis.label_template.format(value=values[axis.name], **values)
                labels.append(slugify(label))
            variant_name = "__".join(labels)
        variants.append(VariantSpec(spec=spec_copy, variant_name=variant_name, axis_values=values))
    return variants
