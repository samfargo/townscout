import numpy as np

def graph_to_csr(G, weight_key="travel_time"):
    nodes = np.array(list(G.nodes), dtype=object)
    nid_to_idx = {n: i for i, n in enumerate(nodes)}
    N = len(nodes)

    rows, cols, wts = [], [], []
    add = rows.append; addc = cols.append; addw = wts.append
    for u, v, k, d in G.edges(keys=True, data=True):
        w = d.get(weight_key)
        if w is not None:
            add(nid_to_idx[u]); addc(nid_to_idx[v]); addw(float(w))

    rows = np.asarray(rows, dtype=np.int64)
    cols = np.asarray(cols, dtype=np.int32)
    wts  = np.asarray(wts,  dtype=np.float32)

    order = np.lexsort((cols, rows))
    rows, cols, wts = rows[order], cols[order], wts[order]

    indptr = np.zeros(N + 1, dtype=np.int64)
    np.add.at(indptr, rows + 1, 1)
    np.cumsum(indptr, out=indptr)

    # integer seconds, robust to NaNs/inf, clip to [0, 65534]
    w = np.nan_to_num(wts, nan=1e12, posinf=1e12, neginf=0.0)
    w = np.minimum(np.ceil(np.maximum(w, 0.0)), 65534.0).astype(np.uint16)

    lats = np.array([float(G.nodes[n]["y"]) for n in nodes], dtype=np.float32)
    lons = np.array([float(G.nodes[n]["x"]) for n in nodes], dtype=np.float32)

    return nodes, indptr, cols, w, lats, lons, nid_to_idx
