import numpy as np
from typing import Tuple

def build_rev_csr(indptr: np.ndarray, indices: np.ndarray, w_sec: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Build the transpose (reverse) of a CSR graph.
    Returns (indptr_rev:int64[N+1], indices_rev:int32[M], w_rev:uint16[M]).
    """
    N = int(indptr.shape[0] - 1)
    M = int(indices.shape[0])
    indptr_rev = np.zeros(N + 1, dtype=np.int64)
    for u in range(N):
        lo, hi = int(indptr[u]), int(indptr[u + 1])
        for v in indices[lo:hi]:
            indptr_rev[int(v) + 1] += 1
    np.cumsum(indptr_rev, out=indptr_rev)
    indices_rev = np.empty(M, dtype=np.int32)
    w_rev = np.empty(M, dtype=np.uint16)
    cursor = indptr_rev.copy()
    for u in range(N):
        lo, hi = int(indptr[u]), int(indptr[u + 1])
        for i in range(lo, hi):
            v = int(indices[i])
            pos = cursor[v]
            indices_rev[pos] = np.int32(u)
            w_rev[pos] = w_sec[i]
            cursor[v] = pos + 1
    return indptr_rev, indices_rev, w_rev

