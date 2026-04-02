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

import itertools
import math
from dataclasses import replace

from .models import ExperimentSpec, VariantSpec
from .utils import deep_copy, dotted_set, format_template, slugify

_DERIVED_BUILTINS = {
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
    "int": int,
    "float": float,
    "math": math,
}


def _eval_derived(expr: str, axis_values: dict) -> object:
    namespace = dict(_DERIVED_BUILTINS)
    namespace.update(axis_values)
    return eval(expr, {"__builtins__": {}}, namespace)  # noqa: S307


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
