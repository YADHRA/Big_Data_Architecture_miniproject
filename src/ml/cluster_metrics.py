"""Cluster evaluation helpers for Stage 13."""

from __future__ import annotations

import numpy as np
from sklearn.metrics import adjusted_rand_score


def adjusted_rand_from_contingency(contingency: np.ndarray) -> float:
    """Compute Adjusted Rand Index from a cluster contingency table."""
    contingency = np.asarray(contingency, dtype=np.int64)

    if contingency.ndim != 2:
        raise ValueError("contingency must be a 2D array")

    n = int(contingency.sum())

    if n < 2:
        return 1.0

    def comb2(x: np.ndarray | int) -> np.ndarray | int:
        return x * (x - 1) // 2

    sum_comb_cells = int(comb2(contingency).sum())
    row_sums = contingency.sum(axis=1)
    col_sums = contingency.sum(axis=0)

    sum_comb_rows = int(comb2(row_sums).sum())
    sum_comb_cols = int(comb2(col_sums).sum())
    total_pairs = int(comb2(n))

    expected_index = (
        sum_comb_rows * sum_comb_cols / total_pairs
        if total_pairs
        else 0.0
    )

    max_index = 0.5 * (sum_comb_rows + sum_comb_cols)

    denominator = max_index - expected_index

    if abs(denominator) < 1e-15:
        return 1.0 if sum_comb_cells == expected_index else 0.0

    return float(
        (sum_comb_cells - expected_index) / denominator
    )


def self_test() -> None:
    """Small correctness tests for the ARI implementation."""

    # Perfect agreement
    contingency = np.array([
        [5, 0],
        [0, 5],
    ])

    result = adjusted_rand_from_contingency(contingency)

    assert abs(result - 1.0) < 1e-12, result

    # Compare against sklearn on a small example
    labels_a = np.array([0, 0, 0, 1, 1, 1])
    labels_b = np.array([1, 1, 1, 0, 0, 0])

    expected = adjusted_rand_score(labels_a, labels_b)

    contingency = np.array([
        [0, 3],
        [3, 0],
    ])

    result = adjusted_rand_from_contingency(contingency)

    assert abs(result - expected) < 1e-12, (result, expected)
