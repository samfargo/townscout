from __future__ import annotations
from typing import Dict, Tuple
import numpy as np
import pandas as pd

def ensure_anchor_int_ids(anchors_df: pd.DataFrame) -> pd.DataFrame:
    """Ensure anchors_df has a stable int32 anchor_int_id column (by site_id order)."""
    if "anchor_int_id" in anchors_df.columns:
        return anchors_df
    out = anchors_df.sort_values("site_id").reset_index(drop=True).copy()
    out["anchor_int_id"] = out.index.astype(np.int32)
    return out

def build_anchor_mappings(anchors_df: pd.DataFrame, node_ids: np.ndarray) -> Tuple[np.ndarray, Dict[int,int]]:
    """Return (anchor_idx:int32[N], nid_to_idx:dict). anchor_idx[j] = anchor_int_id for node j, else -1."""
    anchors_df = ensure_anchor_int_ids(anchors_df)
    nid_to_idx = {int(n): i for i, n in enumerate(node_ids.tolist())}
    anchor_idx = np.full(len(node_ids), -1, dtype=np.int32)
    for node_id, aint in anchors_df[["node_id", "anchor_int_id"]].itertuples(index=False):
        j = nid_to_idx.get(int(node_id))
        if j is not None:
            anchor_idx[j] = int(aint)
    return anchor_idx, nid_to_idx

