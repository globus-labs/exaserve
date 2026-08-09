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
import operator

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
    for name in ("ceil", "floor", "sqrt", "log", "log2", "log10", "pow", "exp", "pi", "e", "inf")
}

_ALLOWED_NODES = (
    ast.Expression,
    ast.Constant,
    ast.Name,
    ast.Load,
    ast.Call,
    ast.BinOp,
    ast.UnaryOp,
    ast.BoolOp,
    ast.Compare,
    ast.IfExp,
    ast.Attribute,
    ast.Tuple,
    ast.List,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.USub,
    ast.UAdd,
    ast.Not,
    ast.And,
    ast.Or,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
)

_BINARY_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY_OPERATORS = {
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
    ast.Not: operator.not_,
}
_COMPARISON_OPERATORS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}
_MAX_EXPRESSION_LENGTH = 1024
_MAX_EXPRESSION_NODES = 128
_MAX_SEQUENCE_LENGTH = 64
_MAX_INTEGER_BITS = 256


def _validate_expression_value(value: object, *, context: str) -> object:
    """Keep the evaluator's value domain finite and free of user objects."""
    if value is None or type(value) in {bool, str}:  # noqa: E721 - exact built-in types
        if isinstance(value, str) and len(value) > 4096:
            raise ValueError(f"{context} produced an overlong string")
        return value
    if type(value) is int:  # bool is deliberately excluded above
        if value.bit_length() > _MAX_INTEGER_BITS:
            raise ValueError(f"{context} produced an integer wider than {_MAX_INTEGER_BITS} bits")
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError(f"{context} produced a non-finite float")
        return value
    if type(value) in {tuple, list}:  # noqa: E721 - exact built-in containers only
        if len(value) > _MAX_SEQUENCE_LENGTH:
            raise ValueError(f"{context} produced too many sequence values")
        checked = [_validate_expression_value(item, context=context) for item in value]
        return tuple(checked) if type(value) is tuple else checked
    raise ValueError(f"{context} contains unsupported value type {type(value).__name__}")


def _interpret_expression(node: ast.AST, axis_values: dict[str, object]) -> object:
    """Interpret the tiny expression language without compiling Python code."""
    if isinstance(node, ast.Expression):
        return _interpret_expression(node.body, axis_values)
    if isinstance(node, ast.Constant):
        return _validate_expression_value(node.value, context="matrix.derived constant")
    if isinstance(node, ast.Name):
        if node.id not in axis_values:
            raise ValueError(f"matrix.derived expression references unknown axis {node.id!r}")
        return axis_values[node.id]
    if isinstance(node, (ast.Tuple, ast.List)):
        values = [_interpret_expression(item, axis_values) for item in node.elts]
        result = tuple(values) if isinstance(node, ast.Tuple) else values
        return _validate_expression_value(result, context="matrix.derived sequence")
    if isinstance(node, ast.Attribute):
        # Shape validation below restricts attributes to this exact namespace.
        value = _ALLOWED_MATH[node.attr]
        if callable(value):
            raise ValueError(f"matrix.derived function math.{node.attr} must be called")
        return _validate_expression_value(value, context=f"matrix.derived math.{node.attr}")
    if isinstance(node, ast.BinOp):
        left = _interpret_expression(node.left, axis_values)
        right = _interpret_expression(node.right, axis_values)
        if isinstance(node.op, ast.Pow) and type(right) in {int, float} and abs(right) > 64:
            raise ValueError("matrix.derived exponent exceeds the finite evaluation bound")
        result = _BINARY_OPERATORS[type(node.op)](left, right)
        return _validate_expression_value(result, context="matrix.derived arithmetic")
    if isinstance(node, ast.UnaryOp):
        result = _UNARY_OPERATORS[type(node.op)](_interpret_expression(node.operand, axis_values))
        return _validate_expression_value(result, context="matrix.derived unary operation")
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            result = _interpret_expression(node.values[0], axis_values)
            for value_node in node.values[1:]:
                if not result:
                    break
                result = _interpret_expression(value_node, axis_values)
            return result
        result = _interpret_expression(node.values[0], axis_values)
        for value_node in node.values[1:]:
            if result:
                break
            result = _interpret_expression(value_node, axis_values)
        return result
    if isinstance(node, ast.Compare):
        left = _interpret_expression(node.left, axis_values)
        for operation, comparator_node in zip(node.ops, node.comparators):
            right = _interpret_expression(comparator_node, axis_values)
            if not _COMPARISON_OPERATORS[type(operation)](left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.IfExp):
        selected = node.body if _interpret_expression(node.test, axis_values) else node.orelse
        return _interpret_expression(selected, axis_values)
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Name):
            function = _DERIVED_FUNCS[node.func.id]
        else:
            function = _ALLOWED_MATH[node.func.attr]
        arguments = [_interpret_expression(argument, axis_values) for argument in node.args]
        result = function(*arguments)
        return _validate_expression_value(result, context="matrix.derived function")
    raise ValueError(f"matrix.derived evaluator has no handler for {type(node).__name__}")


def _eval_derived(expr: str, axis_values: dict) -> object:
    """AST-restricted expression evaluation (PR-016).

    A spec expression may use numeric/string literals, declared axis names,
    arithmetic/comparison/conditional operators, the approved functions
    (min/max/abs/round/int/float), and ``math.<approved>``. Everything else
    — attribute traversal, subscripts, lambdas, comprehensions, dunder
    access — is rejected. YAML specs are configuration, not code.
    """
    if not isinstance(expr, str) or not expr or len(expr) > _MAX_EXPRESSION_LENGTH:
        raise ValueError(
            f"matrix.derived expression must be 1..{_MAX_EXPRESSION_LENGTH} characters"
        )
    if not isinstance(axis_values, dict) or any(
        not isinstance(name, str) or not name.isidentifier() or name.startswith("__")
        for name in axis_values
    ):
        raise ValueError("matrix.derived axis values must use non-dunder identifier names")
    checked_axis_values = {
        name: _validate_expression_value(value, context=f"matrix axis {name!r}")
        for name, value in axis_values.items()
    }
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"matrix.derived expression invalid: {expr!r}: {exc}") from exc

    nodes = list(ast.walk(tree))
    if len(nodes) > _MAX_EXPRESSION_NODES:
        raise ValueError(f"matrix.derived expression exceeds {_MAX_EXPRESSION_NODES} syntax nodes")
    for node in nodes:
        if not isinstance(node, _ALLOWED_NODES):
            raise ValueError(
                f"matrix.derived expression {expr!r} uses disallowed syntax {type(node).__name__}"
            )
        if isinstance(node, ast.Attribute):
            if not (
                isinstance(node.value, ast.Name)
                and node.value.id == "math"
                and node.attr in _ALLOWED_MATH
            ):
                raise ValueError(
                    f"matrix.derived expression {expr!r}: only math.<{'/'.join(sorted(_ALLOWED_MATH))}> attributes are allowed"
                )
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise ValueError(f"matrix.derived expression {expr!r}: dunder names forbidden")
        if isinstance(node, ast.Call):
            func = node.func
            ok = (isinstance(func, ast.Name) and func.id in _DERIVED_FUNCS) or (
                isinstance(func, ast.Attribute)
                and isinstance(func.value, ast.Name)
                and func.value.id == "math"
                and func.attr in _ALLOWED_MATH
            )
            if not ok:
                raise ValueError(
                    f"matrix.derived expression {expr!r}: only "
                    f"{sorted(_DERIVED_FUNCS)} and math.<fn> calls are allowed"
                )

    try:
        return _validate_expression_value(
            _interpret_expression(tree, checked_axis_values),
            context="matrix.derived result",
        )
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise ValueError(f"matrix.derived expression {expr!r} failed: {exc}") from exc


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

        # Matrix values are untyped YAML data until assignment.  Revalidate the
        # concrete variant so a string/bool in an integer field cannot survive
        # materialization under a plausible-looking name.
        from .spec_io import validate_experiment_spec

        validate_experiment_spec(spec_copy)

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
