"""Matrix expansion: combinatorial sweep over experiment axes.

Given an ExperimentSpec with a MatrixSpec (e.g., axes = [{name: num_nodes,
values: [1,2,4]}]), expand_matrix() produces one VariantSpec per point in
the Cartesian product. Each variant gets a deep-copied spec with the axis
values injected into the targeted dotted paths (e.g., "deployment.num_nodes").

Axes can target any field on the spec via dotted_set(). Fields *not* targeted
by the matrix (like client.num_nodes, scheduler.nodes) are auto-synced to
deployment.num_nodes so they stay consistent unless explicitly swept.
"""

from __future__ import annotations

import itertools
from dataclasses import replace

from .models import ExperimentSpec, VariantSpec
from .utils import deep_copy, dotted_set, format_template, slugify


def expand_matrix(spec: ExperimentSpec) -> list[VariantSpec]:
    if not spec.matrix.axes:
        return [VariantSpec(spec=deep_copy(spec), variant_name="default", axis_values={})]

    axis_names = [axis.name for axis in spec.matrix.axes]
    axis_values = [axis.values for axis in spec.matrix.axes]
    variants = []
    for combination in itertools.product(*axis_values):
        values = dict(zip(axis_names, combination))
        spec_copy = deep_copy(spec)
        for axis in spec_copy.matrix.axes:
            value = values[axis.name]
            for target in axis.targets:
                dotted_set(spec_copy, target, value)

        targeted_fields = {t for axis in spec.matrix.axes for t in axis.targets}
        if "client.num_nodes" not in targeted_fields:
            spec_copy.client.num_nodes = spec_copy.deployment.num_nodes
        if "scheduler.nodes" not in targeted_fields:
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
