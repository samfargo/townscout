mod ch;

use numpy::{PyArray1, PyArray2, PyReadonlyArray1, PyArrayMethods};
use pyo3::prelude::*;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::cmp::min;
use rustc_hash::FxHashMap;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use h3o::{CellIndex, LatLng, Resolution};

const UNREACHABLE: u16 = 65535;

fn insert_label_for_node(
    node_i: usize,
    k: usize,
    src_idx: i32,
    t: u16,
    best_src: &mut [i32],
    time_s: &mut [u16],
    labels_used: &mut [u8],
    primary_count: &mut [u8],
    cutoff_primary: u16,
) {
    let base = node_i * k;
    let used = labels_used[node_i] as usize;

    // Find if src already present
    let mut found_j: Option<usize> = None;
    for j in 0..used {
        if best_src[base + j] == src_idx { found_j = Some(j); break; }
    }

    // helper: bubble element at position pos left while times are out of order
    fn bubble_left_inner(base: usize, pos: usize, best_src: &mut [i32], time_s: &mut [u16]) {
        let mut j = pos;
        while j > 0 {
            let tj = time_s[base + j];
            let tjm1 = time_s[base + j - 1];
            if tj < tjm1 {
                time_s[base + j] = tjm1;
                time_s[base + j - 1] = tj;
                let sj = best_src[base + j];
                let sjm1 = best_src[base + j - 1];
                best_src[base + j] = sjm1;
                best_src[base + j - 1] = sj;
                j -= 1;
            } else { break; }
        }
    }

    match found_j {
        Some(j) => {
            let old_t = time_s[base + j];
            if t < old_t {
                time_s[base + j] = t;
                if old_t > cutoff_primary && t <= cutoff_primary {
                    primary_count[node_i] = primary_count[node_i].saturating_add(1);
                }
                bubble_left_inner(base, j, best_src, time_s);
            }
        }
        None => {
            let t_is_primary = t <= cutoff_primary;
            if used < k {
                // Insert into sorted position by shifting
                let mut ins = used;
                for j in 0..used { if t < time_s[base + j] { ins = j; break; } }
                let mut j2 = used;
                while j2 > ins {
                    time_s[base + j2] = time_s[base + j2 - 1];
                    best_src[base + j2] = best_src[base + j2 - 1];
                    j2 -= 1;
                }
                time_s[base + ins] = t;
                best_src[base + ins] = src_idx;
                labels_used[node_i] = (used as u8) + 1;
                if t_is_primary { primary_count[node_i] = primary_count[node_i].saturating_add(1); }
            } else {
                // used == k: replacement logic
                if t_is_primary {
                    let worst_idx = k - 1;
                    let worst_t = time_s[base + worst_idx];
                    if t < worst_t {
                        let replaced_primary = worst_t <= cutoff_primary;
                        time_s[base + worst_idx] = t;
                        best_src[base + worst_idx] = src_idx;
                        if !replaced_primary { primary_count[node_i] = primary_count[node_i].saturating_add(1); }
                        bubble_left_inner(base, worst_idx, best_src, time_s);
                    }
                } else {
                    if (primary_count[node_i] as usize) < k {
                        // Replace worst overflow only
                        let mut worst_over_idx: Option<usize> = None;
                        let mut worst_over_t: u16 = 0;
                        for j in 0..k {
                            let tj = time_s[base + j];
                            if tj > cutoff_primary {
                                if worst_over_idx.is_none() || tj > worst_over_t { worst_over_idx = Some(j); worst_over_t = tj; }
                            }
                        }
                        if let Some(widx) = worst_over_idx {
                            if t < time_s[base + widx] {
                                time_s[base + widx] = t;
                                best_src[base + widx] = src_idx;
                                bubble_left_inner(base, widx, best_src, time_s);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[pyfunction]
fn kbest_multisource_csr(
    py: Python,
    indptr: PyReadonlyArray1<i64>,
    indices: PyReadonlyArray1<i32>,
    w_sec: PyReadonlyArray1<u16>,
    source_idxs: PyReadonlyArray1<i32>,
    k: usize,
    cutoff_s: u16,
    _threads: usize, // Parameter is unused for now in this single-threaded version
) -> PyResult<(Py<PyArray2<i32>>, Py<PyArray2<u16>>)> {
    let indptr = indptr.as_slice()?;
    let indices = indices.as_slice()?;
    let w_sec = w_sec.as_slice()?;
    let source_idxs = source_idxs.as_slice()?;

    let n_nodes = indptr.len() - 1;

    // State for Dijkstra's algorithm
    // A max-heap for each node to store the K best times.
    // We store (time, source_node_index).
    let mut best_results: Vec<BinaryHeap<(u16, i32)>> = vec![BinaryHeap::with_capacity(k); n_nodes];
    
    // The main priority queue for the search. Min-heap.
    // We store Reverse((time, current_node_index, source_node_index)).
    let mut pq: BinaryHeap<Reverse<(u16, i32, i32)>> = BinaryHeap::new();

    // Initialize PQ with all source nodes
    for &src_idx in source_idxs {
        pq.push(Reverse((0, src_idx, src_idx)));
    }

    while let Some(Reverse((time, u_idx, src_idx))) = pq.pop() {
        if time > cutoff_s {
            continue;
        }

        // Check if this path is a candidate for the K-best list for node u_idx
        let u_bests = &mut best_results[u_idx as usize];
        if u_bests.len() < k {
            u_bests.push((time, src_idx));
        } else {
            // If the heap is full, check if the new time is better than the worst (max) time
            if let Some(mut worst) = u_bests.peek_mut() {
                if time < worst.0 {
                    *worst = (time, src_idx);
                } else {
                    // This path is not better than any of the K best, but we still need to
                    // relax its edges as a shorter path to other nodes might exist through it.
                }
            }
        }

        // Relax edges
        let start = indptr[u_idx as usize] as usize;
        let end = indptr[(u_idx + 1) as usize] as usize;
        for i in start..end {
            let v_idx = indices[i];
            let weight = w_sec[i];
            let new_time = time + weight;

            if new_time < cutoff_s {
                 // Optimization: if we have K results for v_idx and the new time is not better than the worst, don't push to PQ.
                let v_bests = &best_results[v_idx as usize];
                if v_bests.len() == k && new_time >= v_bests.peek().unwrap().0 {
                    continue;
                }
                pq.push(Reverse((new_time, v_idx, src_idx)));
            }
        }
    }

    // Prepare output arrays
    let (best_src_idx_out, time_s_out) = unsafe {
        let best_src_idx_out = PyArray2::new_bound(py, [n_nodes, k], false);
        let time_s_out = PyArray2::new_bound(py, [n_nodes, k], false);
        (best_src_idx_out, time_s_out)
    };
    
    let best_src_idx_out_slice = unsafe { best_src_idx_out.as_slice_mut()? };
    let time_s_out_slice = unsafe { time_s_out.as_slice_mut()? };

    // Fill with sentinels
    best_src_idx_out_slice.fill(-1);
    time_s_out_slice.fill(UNREACHABLE);

    for (i, heap) in best_results.into_iter().enumerate() {
        let sorted_bests: Vec<(u16, i32)> = heap.into_sorted_vec();
        for (j, (time, src_idx)) in sorted_bests.into_iter().enumerate() {
             let idx = i * k + j;
             best_src_idx_out_slice[idx] = src_idx;
             time_s_out_slice[idx] = time;
        }
    }
    
    Ok((best_src_idx_out.into(), time_s_out.into()))
}

/// Bucket-based multi-source single-label SSSP (Dial's algorithm) composed into K-pass to build K-best labels.
/// Two-stage cutoffs: primary for standard search, then overflow for nodes that did not reach K labels.
/// indptr: CSR row pointers (len=N+1, i64), indices: CSR column indices (len=M, i32), w_sec: edge weights (u16 seconds)
/// source_idxs: CSR node indices of anchor sources (i32)
///
/// Parallelization strategy (when threads > 1):
/// - Partition `source_idxs` into T disjoint chunks.
/// - Run the exact same single-thread Dial SSSP per chunk in parallel.
/// - Each chunk emits per-node local top-K (size K), already deduped per (node,src).
/// - Deterministically merge per-node across chunks into global top-K using the same
///   `insert_label_for_node` semantics, iterating candidates in (time, src_idx) order.
/// This avoids shared mutable state during relaxations and preserves correctness.
#[pyfunction(signature = (
    indptr, indices, w_sec, source_idxs, k, cutoff_primary_s, cutoff_overflow_s, threads, progress, progress_cb=None, targets_idx=None
))]
fn kbest_multisource_bucket_csr(
    py: Python,
    indptr: PyReadonlyArray1<i64>,
    indices: PyReadonlyArray1<i32>,
    w_sec: PyReadonlyArray1<u16>,
    source_idxs: PyReadonlyArray1<i32>,
    k: usize,
    cutoff_primary_s: u16,
    cutoff_overflow_s: u16,
    threads: usize,
    progress: bool,
    progress_cb: Option<PyObject>,
    targets_idx: Option<PyReadonlyArray1<i32>>,
) -> PyResult<(Py<PyArray2<i32>>, Py<PyArray2<u16>>)> {
    let indptr = indptr.as_slice()?;
    let indices = indices.as_slice()?;
    let w_sec = w_sec.as_slice()?;
    let source_idxs = source_idxs.as_slice()?;

    let n_nodes: usize = indptr.len() - 1;
    // Helper: single-chunk computation
    fn compute_chunk(
        indptr: &[i64],
        indices: &[i32],
        w_sec: &[u16],
        min_out: &[u16],
        source_idxs: &[i32],
        n_nodes: usize,
        k: usize,
        cutoff_primary_s: u16,
        cutoff_overflow_s: u16,
        log_progress: bool,
        is_target: Option<&[u8]>,
        targets_total: usize,
    ) -> (Vec<i32>, Vec<u16>) {
        use std::time::{Instant, Duration};

        let mut best_src_idx_out: Vec<i32> = vec![-1; n_nodes * k];
        let mut time_s_out: Vec<u16> = vec![UNREACHABLE; n_nodes * k];
        let mut labels_used: Vec<u8> = vec![0u8; n_nodes];
        let mut primary_count: Vec<u8> = vec![0u8; n_nodes];

        let buckets_len = (cutoff_overflow_s as usize) + 1;
        let mut buckets: Vec<Vec<(i32, i32)>> = vec![Vec::new(); buckets_len];
        let mut active: Vec<bool> = vec![false; buckets_len];
        // Dial ring-pointer frontier (no heap)
        let mut active_count: usize = 0;
        let mut cur_idx: usize = 0;
        let mut pair_best: FxHashMap<u64, u16> = FxHashMap::default();
        pair_best.reserve(source_idxs.len() * 8);

        for &s in source_idxs {
            buckets[0].push((s, s));
        }
        if !buckets[0].is_empty() && !active[0] { active[0] = true; active_count += 1; }

        let start_ts = Instant::now();
        let mut last_log = start_ts;
        let log_every = Duration::from_secs(5);
        let mut pops: usize = 0;
        let mut prim_assigned: usize = 0;
        let mut nodes_full_primary: usize = 0;
        let mut remaining_targets: isize = targets_total as isize;
        while active_count > 0 {
            if !active[cur_idx] || buckets[cur_idx].is_empty() {
                if active[cur_idx] && buckets[cur_idx].is_empty() {
                    active[cur_idx] = false;
                    active_count -= 1;
                }
                cur_idx = (cur_idx + 1) % buckets_len;
                continue;
            }
            let (u_idx, src_idx) = buckets[cur_idx].pop().unwrap();
            let du = cur_idx as u16;
            let ui = u_idx as usize;
            pops += 1;

            // prune per (node,src)
            let key: u64 = ((ui as u64) << 32) | (src_idx as u32 as u64);
            if let Some(&best) = pair_best.get(&key) { if du >= best { continue; } }
            pair_best.insert(key, du);

            // If node already has K primary labels with worst <= du,
            // no need to record or relax neighbors for this (u, src, du).
            if (primary_count[ui] as usize) == k {
                let wp = time_s_out[ui * k + (k - 1)];
                if du >= wp { continue; }
                // If even the cheapest outgoing edge can't improve any neighbor, skip relaxing
                let m = min_out[ui];
                if m > 0 && du.saturating_add(m) >= wp { continue; }
            }

            // record label under primary/overflow rules
            let before_primary = primary_count[ui] as usize;
            let before_used = labels_used[ui] as usize;
            insert_label_for_node(
                ui,
                k,
                src_idx,
                du,
                &mut best_src_idx_out,
                &mut time_s_out,
                &mut labels_used,
                &mut primary_count,
                cutoff_primary_s,
            );
            if du <= cutoff_primary_s {
                prim_assigned += 1;
                if before_primary < k && (primary_count[ui] as usize) == k { nodes_full_primary += 1; }
            }

            // Target-aware early stop: when a target receives its first label, decrement
            if before_used == 0 {
                if let Some(mask) = is_target {
                    if mask[ui] != 0 {
                        remaining_targets -= 1;
                        if remaining_targets == 0 { break; }
                    }
                }
            }

            // relax neighbors
            let start = indptr[ui] as usize;
            let end = indptr[ui + 1] as usize;
            for e in start..end {
                let v = indices[e] as usize;
                let w = w_sec[e];
                let nd = du.saturating_add(w);
                if nd > cutoff_overflow_s { continue; }
                // Early prune: if v already has K primary labels and this candidate
                // is not better than v's current worst primary label, skip.
                if (primary_count[v] as usize) == k {
                    let worst_p = time_s_out[v * k + (k - 1)];
                    if nd >= worst_p { continue; }
                }
                let vkey: u64 = ((v as u64) << 32) | (src_idx as u32 as u64);
                if let Some(&best) = pair_best.get(&vkey) { if nd >= best { continue; } }
                let nd_us = nd as usize;
                buckets[nd_us].push((indices[e], src_idx));
                if !active[nd_us] {
                    active[nd_us] = true; active_count += 1;
                }
            }

            // periodic live logging from within chunk
            if log_progress && last_log.elapsed() >= log_every {
                let elapsed = start_ts.elapsed().as_secs();
                eprintln!(
                    "[kbest:chunk] t={}s cur={} pops={} pairs={} prim_labels={} nodes_full_k={}",
                    elapsed, cur_idx, pops, pair_best.len(), prim_assigned, nodes_full_primary
                );
                last_log = Instant::now();
            }
            // After processing one item, if current bucket emptied, deactivate
            if buckets[cur_idx].is_empty() && active[cur_idx] {
                active[cur_idx] = false;
                active_count -= 1;
            }
        }

        (best_src_idx_out, time_s_out)
    }

    // Threads handling
    let t = if threads == 0 { 1 } else { threads };
    let should_parallel = t > 1 && source_idxs.len() > t * 4; // heuristic: enough work per thread

    // Precompute per-node minimal outgoing edge for neighbor-prune
    let mut min_out: Vec<u16> = vec![u16::MAX; n_nodes];
    for ui in 0..n_nodes {
        let start = indptr[ui] as usize;
        let end = indptr[ui + 1] as usize;
        let mut m = u16::MAX;
        for e in start..end { let w = w_sec[e]; if w < m { m = w; } }
        if m == u16::MAX { m = 0; }
        min_out[ui] = m;
    }

    // Build optional target mask
    let (target_mask_opt, targets_total): (Option<Vec<u8>>, usize) = if let Some(tidx) = &targets_idx {
        let arr = tidx.as_slice()?;
        let mut mask: Vec<u8> = vec![0u8; n_nodes];
        for &u in arr.iter() {
            let ui = u as usize;
            if ui < n_nodes { mask[ui] = 1; }
        }
        (Some(mask), arr.len())
    } else { (None, 0usize) };

    if !should_parallel {
        // Single-chunk path (backwards compatible)
        let (best_src_idx_vec, time_s_vec) = py.allow_threads(|| compute_chunk(
            indptr, indices, w_sec, &min_out, source_idxs, n_nodes, k, cutoff_primary_s, cutoff_overflow_s, progress,
            target_mask_opt.as_deref(), targets_total,
        ));
        if progress {
            if let Some(cb) = &progress_cb {
                Python::with_gil(|py| { let _ = cb.call1(py, (1usize, 1usize)); });
            } else {
                eprintln!("[kbest] chunk {}/{}", 1, 1);
            }
        }

        // Prepare numpy outputs
        let (best_src_idx_out, time_s_out) = unsafe {
            let best_src_idx_out = PyArray2::new_bound(py, [n_nodes, k], false);
            let time_s_out = PyArray2::new_bound(py, [n_nodes, k], false);
            (best_src_idx_out, time_s_out)
        };
        let best_src_idx_out_slice = unsafe { best_src_idx_out.as_slice_mut()? };
        let time_s_out_slice = unsafe { time_s_out.as_slice_mut()? };
        best_src_idx_out_slice.copy_from_slice(&best_src_idx_vec);
        time_s_out_slice.copy_from_slice(&time_s_vec);
        return Ok((best_src_idx_out.into(), time_s_out.into()));
    }

    // Parallel path: partition sources and compute per-chunk results
    // Use moderate chunking to balance progress with memory usage
    let num_parts = min(t.saturating_mul(2), source_idxs.len());

    // Build explicit chunk slices
    let mut parts: Vec<(usize, usize)> = Vec::with_capacity(num_parts);
    let mut start = 0usize;
    for i in 0..num_parts {
        let remain = source_idxs.len() - start;
        let chunk_len = (remain + (num_parts - i) - 1) / (num_parts - i); // ceil division
        parts.push((start, start + chunk_len));
        start += chunk_len;
    }

    // Scoped thread pool to honor `threads`
    let pool = ThreadPoolBuilder::new().num_threads(t).build().map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to build thread pool: {}", e)))?;

    use std::sync::{Arc};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let total_parts = parts.len();
    let counter = Arc::new(AtomicUsize::new(0));
    let chunk_results: Vec<(Vec<i32>, Vec<u16>)> = py.allow_threads(|| pool.install(|| {
        parts
            .par_iter()
            .map(|&(lo, hi)| {
                let slice = &source_idxs[lo..hi];
                // For chunked path, we cannot early stop globally across chunks; pass mask but it won't hit zero typically
                let res = compute_chunk(indptr, indices, w_sec, &min_out, slice, n_nodes, k, cutoff_primary_s, cutoff_overflow_s, progress,
                    target_mask_opt.as_deref(), targets_total);
                if progress {
                    let done = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Some(cb) = &progress_cb {
                        Python::with_gil(|py| { let _ = cb.call1(py, (done, total_parts)); });
                    } else {
                        eprintln!("[kbest] chunk {}/{}", done, total_parts);
                    }
                }
                res
            })
            .collect()
    }));

    // Global merge per node
    let (best_src_idx_out, time_s_out) = unsafe {
        let best_src_idx_out = PyArray2::new_bound(py, [n_nodes, k], false);
        let time_s_out = PyArray2::new_bound(py, [n_nodes, k], false);
        (best_src_idx_out, time_s_out)
    };
    let best_src_idx_out_slice = unsafe { best_src_idx_out.as_slice_mut()? };
    let time_s_out_slice = unsafe { time_s_out.as_slice_mut()? };
    best_src_idx_out_slice.fill(-1);
    time_s_out_slice.fill(UNREACHABLE);

    let mut labels_used: Vec<u8> = vec![0u8; n_nodes];
    let mut primary_count: Vec<u8> = vec![0u8; n_nodes];

    // Reusable small buffer for candidates per node
    let mut candidates: Vec<(u16, i32)> = Vec::with_capacity(num_parts * k);

    for node_i in 0..n_nodes {
        candidates.clear();
        let base = node_i * k;

        for (bs, ts) in chunk_results.iter().map(|(b, t)| (b, t)) {
            for j in 0..k {
                let s = bs[base + j];
                if s < 0 { continue; }
                let t = ts[base + j];
                if t >= UNREACHABLE { continue; }
                candidates.push((t, s));
            }
        }

        // sort by (time, src_idx) for deterministic merge
        candidates.sort_by(|a, b| {
            let ord_t = a.0.cmp(&b.0);
            if ord_t != std::cmp::Ordering::Equal { return ord_t; }
            a.1.cmp(&b.1)
        });

        for (t, s) in &candidates {
            insert_label_for_node(
                node_i,
                k,
                *s,
                *t,
                best_src_idx_out_slice,
                time_s_out_slice,
                &mut labels_used,
                &mut primary_count,
                cutoff_primary_s,
            );
        }
    }

    Ok((best_src_idx_out.into(), time_s_out.into()))
}

/// Compute weakly connected components using both forward and reverse adjacency.
/// Returns an array `comp_id` of length N (int32) with component indices [0..n_components).
#[pyfunction]
fn weakly_connected_components(
    _py: Python,
    indptr: PyReadonlyArray1<i64>,
    indices: PyReadonlyArray1<i32>,
    indptr_rev: PyReadonlyArray1<i64>,
    indices_rev: PyReadonlyArray1<i32>,
) -> PyResult<Py<PyArray1<i32>>> {
    use std::collections::VecDeque;
    let indptr = indptr.as_slice()?;
    let indices = indices.as_slice()?;
    let indptr_rev = indptr_rev.as_slice()?;
    let indices_rev = indices_rev.as_slice()?;
    let n_nodes: usize = indptr.len() - 1;
    let mut comp_id: Vec<i32> = vec![-1; n_nodes];
    let mut cid: i32 = 0;
    for start in 0..n_nodes {
        if comp_id[start] >= 0 { continue; }
        let mut q: VecDeque<usize> = VecDeque::new();
        comp_id[start] = cid;
        q.push_back(start);
        while let Some(u) = q.pop_front() {
            // forward neighbors
            let s = indptr[u] as usize;
            let e = indptr[u + 1] as usize;
            for idx in s..e {
                let v = indices[idx] as usize;
                if comp_id[v] < 0 { comp_id[v] = cid; q.push_back(v); }
            }
            // reverse neighbors
            let s2 = indptr_rev[u] as usize;
            let e2 = indptr_rev[u + 1] as usize;
            for idx in s2..e2 {
                let v = indices_rev[idx] as usize;
                if comp_id[v] < 0 { comp_id[v] = cid; q.push_back(v); }
            }
        }
        cid += 1;
    }

    let arr = PyArray1::from_vec_bound(_py, comp_id);
    Ok(arr.into())
}

/// Aggregate node-level labels (best anchors and times) into H3 hex buckets with per-hex top-K reduction.
/// Inputs:
/// - lats, lons: node coordinates (deg), length N
/// - best_anchor_int: [N,K] site IDs (int32), -1 if missing
/// - time_s: [N,K] times (u16), UNREACHABLE sentinel if missing
/// - resolutions: list of H3 resolutions (int32)
/// - k: number of labels to keep per hex
/// - unreachable: sentinel value (u16) that indicates missing time
/// - threads: number of threads (>=1)
/// Output: flat arrays representing a long-format table across all resolutions
#[pyfunction]
fn aggregate_h3_topk(
    py: Python,
    lats: PyReadonlyArray1<f32>,
    lons: PyReadonlyArray1<f32>,
    best_anchor_int: numpy::PyReadonlyArray2<i32>,
    time_s: numpy::PyReadonlyArray2<u16>,
    resolutions: PyReadonlyArray1<i32>,
    k: usize,
    unreachable: u16,
    threads: usize,
) -> PyResult<(
    Py<PyArray1<u64>>, // h3_id
    Py<PyArray1<i32>>, // site_id
    Py<PyArray1<u16>>, // time_s
    Py<PyArray1<i32>>, // res
)> {
    use rustc_hash::FxHashMap;
    // removed unused Ordering import (we use fully-qualified paths where needed)
    let lats = lats.as_slice()?;
    let lons = lons.as_slice()?;
    let a = best_anchor_int.as_array();
    let time_arr = time_s.as_array();
    let n_nodes = a.shape()[0];
    let kdim = a.shape()[1];
    if time_arr.shape()[0] != n_nodes || time_arr.shape()[1] != kdim {
        return Err(pyo3::exceptions::PyValueError::new_err("time_s shape must match best_anchor_int"));
    }
    if lats.len() != n_nodes || lons.len() != n_nodes {
        return Err(pyo3::exceptions::PyValueError::new_err("lats/lons length must match number of nodes"));
    }
    let res_list = resolutions.as_slice()?;
    if res_list.is_empty() { return Err(pyo3::exceptions::PyValueError::new_err("resolutions must be non-empty")); }

    // Convert to h3o Resolutions up-front
    let mut res_objs: Vec<Resolution> = Vec::with_capacity(res_list.len());
    for &r in res_list {
        let rr: u8 = if r < 0 { return Err(pyo3::exceptions::PyValueError::new_err("resolution must be >=0")); } else { r as u8 };
        let ro = Resolution::try_from(rr).map_err(|_| pyo3::exceptions::PyValueError::new_err(format!("invalid H3 resolution: {}", r)))?;
        res_objs.push(ro);
    }

    let threads_n = if threads == 0 { 1 } else { threads };
    let num_parts = std::cmp::min(threads_n, std::cmp::max(1, n_nodes));

    // Partition nodes into ranges
    let mut parts: Vec<(usize, usize)> = Vec::with_capacity(num_parts);
    let mut start = 0usize;
    for i in 0..num_parts {
        let remain = n_nodes - start;
        let chunk_len = (remain + (num_parts - i) - 1) / (num_parts - i);
        parts.push((start, start + chunk_len));
        start += chunk_len;
    }

    // Each part returns Vec<HashMap<h3_id, TopK(site_id,time)>> of length R
    type TopK = Vec<(i32, u16)>; // kept small (<=k)
    type HexMap = FxHashMap<u64, TopK>;

    #[inline]
    fn update_topk(vec: &mut TopK, site: i32, ts: u16, k: usize) {
        use std::cmp::Ordering;
        // If site exists, keep its minimum time
        for p in vec.iter_mut() {
            if p.0 == site {
                if ts < p.1 { p.1 = ts; }
                // Re-establish order after improvement
                vec.sort_unstable_by(|x, y| {
                    let o = x.1.cmp(&y.1);
                    if o != Ordering::Equal { return o; }
                    x.0.cmp(&y.0)
                });
                return;
            }
        }
        if vec.len() < k {
            vec.push((site, ts));
            vec.sort_unstable_by(|x, y| {
                let o = x.1.cmp(&y.1);
                if o != Ordering::Equal { return o; }
                x.0.cmp(&y.0)
            });
            return;
        }
        // vec full; replace worst if better
        let mut worst_i = 0usize;
        let mut worst_t = 0u16;
        for (i, &(_, t)) in vec.iter().enumerate() {
            if i == 0 || t > worst_t { worst_i = i; worst_t = t; }
        }
        if ts < worst_t {
            vec[worst_i] = (site, ts);
            vec.sort_unstable_by(|x, y| {
                let o = x.1.cmp(&y.1);
                if o != Ordering::Equal { return o; }
                x.0.cmp(&y.0)
            });
        }
    }

    let pool = ThreadPoolBuilder::new().num_threads(threads_n).build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to build thread pool: {}", e)))?;

    let partials: Vec<Vec<HexMap>> = py.allow_threads(|| pool.install(|| {
        parts.par_iter().map(|&(lo, hi)| {
            let mut local: Vec<HexMap> = (0..res_objs.len()).map(|_| FxHashMap::default()).collect();
            for i in lo..hi {
                let lat = lats[i] as f64;
                let lon = lons[i] as f64;
                // Skip if out of bounds for numerical safety
                if !(lat.is_finite() && lon.is_finite()) { continue; }
                let ll = match LatLng::new(lat, lon) { Ok(v) => v, Err(_) => continue };
                for (ri, &res) in res_objs.iter().enumerate() {
                    let cell = ll.to_cell(res);
                    let h3_id: u64 = cell.into();
                    let map_for_res = local.get_mut(ri).unwrap();
                    let entry = map_for_res.entry(h3_id).or_insert_with(|| Vec::with_capacity(k));
                    // iterate labels for this node; keep per-hex top-K online
                    for j in 0..kdim {
                        let site = a[[i, j]];
                        if site < 0 { continue; }
                        let ts = time_arr[[i, j]];
                        if ts >= unreachable { continue; }
                        update_topk(entry, site, ts, k);
                    }
                }
            }
            local
        }).collect()
    }));

    // Merge partials into global per-resolution maps
    let mut globals: Vec<HexMap> = (0..res_objs.len()).map(|_| FxHashMap::default()).collect();
    for pr in partials.into_iter() {
        for (ri, hm) in pr.into_iter().enumerate() {
            let g = globals.get_mut(ri).unwrap();
            for (h3_id, inner) in hm.into_iter() {
                let gin = g.entry(h3_id).or_insert_with(|| Vec::with_capacity(k));
                for (site, ts) in inner.into_iter() {
                    update_topk(gin, site, ts, k);
                }
            }
        }
    }

    // Build output vectors
    let mut out_h: Vec<u64> = Vec::new();
    let mut out_s: Vec<i32> = Vec::new();
    let mut out_t: Vec<u16> = Vec::new();
    let mut out_r: Vec<i32> = Vec::new();

    for (ri, g) in globals.iter().enumerate() {
        let r_val: i32 = res_list[ri];
        for (h3_id, pairs) in g.iter() {
            // Ensure sorted (time asc, then site) and emit all (<=K)
            let mut tmp = pairs.clone();
            tmp.sort_unstable_by(|x, y| {
                let o = x.1.cmp(&y.1);
                if o != std::cmp::Ordering::Equal { return o; }
                x.0.cmp(&y.0)
            });
            out_h.reserve(tmp.len());
            out_s.reserve(tmp.len());
            out_t.reserve(tmp.len());
            out_r.reserve(tmp.len());
            for (site, ts) in tmp.into_iter() {
                out_h.push(*h3_id);
                out_s.push(site);
                out_t.push(ts);
                out_r.push(r_val);
            }
        }
    }

    // Convert to numpy arrays
    let h_arr = PyArray1::from_vec_bound(py, out_h);
    let s_arr = PyArray1::from_vec_bound(py, out_s);
    let t_arr = PyArray1::from_vec_bound(py, out_t);
    let r_arr = PyArray1::from_vec_bound(py, out_r);
    Ok((h_arr.into(), s_arr.into(), t_arr.into(), r_arr.into()))
}

/// Compute H3 cell IDs for nodes at the requested resolutions. Returns [N,R] u64.
#[pyfunction]
fn compute_h3_for_nodes(
    py: Python,
    lats: PyReadonlyArray1<f32>,
    lons: PyReadonlyArray1<f32>,
    resolutions: PyReadonlyArray1<i32>,
    threads: usize,
    progress: bool,
) -> PyResult<Py<PyArray2<u64>>> {
    let lats = lats.as_slice()?;
    let lons = lons.as_slice()?;
    let res_list = resolutions.as_slice()?;
    if res_list.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err("resolutions must be non-empty"));
    }
    let n_nodes = lats.len();
    let r_len = res_list.len();
    let mut res_entries: Vec<(usize, u8)> = Vec::with_capacity(r_len);
    for (idx, &val) in res_list.iter().enumerate() {
        let rr = u8::try_from(val).map_err(|_| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid resolution {}", val))
        })?;
        res_entries.push((idx, rr));
    }
    let mut unique_res: Vec<u8> = res_entries.iter().map(|(_, r)| *r).collect();
    unique_res.sort_unstable();
    unique_res.dedup();
    unique_res.sort_unstable_by(|a, b| b.cmp(a));
    let max_res_u8 = unique_res[0];
    let threads_n = if threads == 0 { 1 } else { threads };
    let res_map_result = py.allow_threads(|| {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let thread_pool = ThreadPoolBuilder::new().num_threads(threads_n).build().unwrap();
        let max_res = Resolution::try_from(max_res_u8)
            .map_err(|_| format!("invalid resolution {}", max_res_u8))?;
        let mut max_cells: Vec<u64> = vec![0u64; n_nodes];
        let counter = Arc::new(AtomicUsize::new(0));
        thread_pool.install(|| {
            max_cells.par_iter_mut().enumerate().for_each(|(i, cell)| {
                let lat = lats[i] as f64;
                let lon = lons[i] as f64;
                if !(lat.is_finite() && lon.is_finite()) {
                    return;
                }
                let ll = match LatLng::new(lat, lon) {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let h3_id: u64 = ll.to_cell(max_res).into();
                *cell = h3_id;
                if progress {
                    let done = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if done % 100000 == 0 || done == n_nodes {
                        eprintln!("[h3] nodes {}/{}", done, n_nodes);
                    }
                }
            });
        });
        let mut res_to_cells: FxHashMap<u8, Vec<u64>> = FxHashMap::default();
        res_to_cells.insert(max_res_u8, max_cells.clone());
        let mut current_cells = max_cells;
        let mut current_res = max_res_u8;
        for &target in unique_res.iter().skip(1) {
            while current_res > target {
                let next_res = current_res - 1;
                let res_obj = Resolution::try_from(next_res)
                    .map_err(|_| format!("invalid resolution {}", next_res))?;
                let next_cells: Vec<u64> = current_cells
                    .par_iter()
                    .map(|&h| {
                        if h == 0 {
                            0u64
                        } else if let Ok(cell) = CellIndex::try_from(h) {
                            match cell.parent(res_obj) {
                                Some(parent) => u64::from(parent),
                                None => 0u64,
                            }
                        } else {
                            0u64
                        }
                    })
                    .collect();
                current_cells = next_cells;
                current_res = next_res;
            }
            res_to_cells.insert(target, current_cells.clone());
        }
        Ok::<_, String>(res_to_cells)
    });
    let res_to_cells = match res_map_result {
        Ok(map) => map,
        Err(err) => {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err));
        }
    };
    let mut out_vec: Vec<u64> = vec![0u64; n_nodes * r_len];
    for &(col_idx, res_u8) in &res_entries {
        let column = res_to_cells.get(&res_u8).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "missing derived cells for resolution {}",
                res_u8
            ))
        })?;
        if column.len() != n_nodes {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "derived column length mismatch",
            ));
        }
        for (row, &val) in column.iter().enumerate() {
            out_vec[row * r_len + col_idx] = val;
        }
    }
    let arr = unsafe { PyArray2::new_bound(py, [n_nodes, r_len], false) };
    let arr_slice = unsafe { arr.as_slice_mut()? };
    arr_slice.copy_from_slice(&out_vec);
    Ok(arr.into())
}

/// Aggregate using precomputed H3 cell IDs [N,R].
#[pyfunction]
fn aggregate_h3_topk_precached(
    py: Python,
    h3_ids: numpy::PyReadonlyArray2<u64>,
    best_anchor_int: numpy::PyReadonlyArray2<i32>,
    time_s: numpy::PyReadonlyArray2<u16>,
    resolutions: PyReadonlyArray1<i32>,
    k: usize,
    unreachable: u16,
    threads: usize,
    progress: bool,
) -> PyResult<(
    Py<PyArray1<u64>>, // h3_id
    Py<PyArray1<i32>>, // site_id
    Py<PyArray1<u16>>, // time_s
    Py<PyArray1<i32>>, // res
)> {
    type TopK = Vec<(i32, u16)>; // (site, time)
    type HexMap = FxHashMap<u64, TopK>;

    let h3_arr = h3_ids.as_array();
    let a = best_anchor_int.as_array();
    let t_arr = time_s.as_array();
    let n_nodes = a.shape()[0];
    let kdim = a.shape()[1];
    let r_len = h3_arr.shape()[1];
    if t_arr.shape()[0] != n_nodes || t_arr.shape()[1] != kdim {
        return Err(pyo3::exceptions::PyValueError::new_err("time_s shape must match best_anchor_int"));
    }
    if h3_arr.shape()[0] != n_nodes { return Err(pyo3::exceptions::PyValueError::new_err("h3_ids row count must match nodes")); }
    let res_list = resolutions.as_slice()?;
    if res_list.len() != r_len { return Err(pyo3::exceptions::PyValueError::new_err("resolutions length must match h3_ids columns")); }
    let threads_n = if threads == 0 { 1 } else { threads };

    let (out_h, out_s, out_t, out_r) = py.allow_threads(|| {
        // Partition nodes
        let num_parts = std::cmp::min(threads_n, std::cmp::max(1, n_nodes));
        let mut parts: Vec<(usize, usize)> = Vec::with_capacity(num_parts);
        let mut start = 0usize;
        for i in 0..num_parts {
            let remain = n_nodes - start;
            let chunk_len = (remain + (num_parts - i) - 1) / (num_parts - i);
            parts.push((start, start + chunk_len));
            start += chunk_len;
        }

        // Per-part aggregation
        let pool = ThreadPoolBuilder::new().num_threads(threads_n).build().unwrap();
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let counter = Arc::new(AtomicUsize::new(0));
        let total_parts = parts.len();
        let partials: Vec<Vec<HexMap>> = pool.install(|| {
            parts.par_iter().map(|&(lo, hi)| {
                let mut local: Vec<HexMap> = (0..r_len).map(|_| FxHashMap::default()).collect();
                for i in lo..hi {
                    for ri in 0..r_len {
                        let h3_id = h3_arr[[i, ri]];
                        if h3_id == 0 { continue; }
                        let map_for_res = local.get_mut(ri).unwrap();
                        let entry = map_for_res.entry(h3_id).or_insert_with(|| Vec::with_capacity(k));
                        for j in 0..kdim {
                            let site = a[[i, j]];
                            if site < 0 { continue; }
                            let ts = t_arr[[i, j]];
                            if ts >= unreachable { continue; }
                            // update_topk inline
                            let mut found = false;
                            for p in entry.iter_mut() {
                                if p.0 == site { if ts < p.1 { p.1 = ts; } found = true; break; }
                            }
                            if !found {
                                if entry.len() < k { entry.push((site, ts)); }
                                else {
                                    // replace worst if better
                                    let mut worst_i = 0usize; let mut worst_t = 0u16;
                                    for (ii, &(_, t)) in entry.iter().enumerate() { if ii==0 || t>worst_t { worst_i=ii; worst_t=t; } }
                                    if ts < worst_t { entry[worst_i] = (site, ts); }
                                }
                            }
                        }
                    }
                }
                if progress {
                    let done = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!("[agg] part {}/{}", done, total_parts);
                }
                local
            }).collect()
        });

        // Merge partials into globals
        let mut globals: Vec<HexMap> = (0..r_len).map(|_| FxHashMap::default()).collect();
        for pr in partials.into_iter() {
            for (ri, hm) in pr.into_iter().enumerate() {
                let g = globals.get_mut(ri).unwrap();
                for (h3_id, inner) in hm.into_iter() {
                    let gin = g.entry(h3_id).or_insert_with(|| Vec::with_capacity(k));
                    for (site, ts) in inner.into_iter() {
                        // same update_topk
                        let mut found = false;
                        for p in gin.iter_mut() {
                            if p.0 == site { if ts < p.1 { p.1 = ts; } found = true; break; }
                        }
                        if !found {
                            if gin.len() < k { gin.push((site, ts)); }
                            else {
                                let mut worst_i = 0usize; let mut worst_t = 0u16;
                                for (ii, &(_, t)) in gin.iter().enumerate() { if ii==0 || t>worst_t { worst_i=ii; worst_t=t; } }
                                if ts < worst_t { gin[worst_i] = (site, ts); }
                            }
                        }
                    }
                }
            }
        }

        // Build outputs
        let mut out_h: Vec<u64> = Vec::new();
        let mut out_s: Vec<i32> = Vec::new();
        let mut out_t: Vec<u16> = Vec::new();
        let mut out_r: Vec<i32> = Vec::new();
        for (ri, g) in globals.iter().enumerate() {
            let r_val: i32 = res_list[ri];
            for (h3_id, pairs) in g.iter() {
                // sort deterministically by (time, site)
                let mut tmp = pairs.clone();
                tmp.sort_unstable_by(|x, y| {
                    let o = x.1.cmp(&y.1); if o != std::cmp::Ordering::Equal { return o; }
                    x.0.cmp(&y.0)
                });
                out_h.reserve(tmp.len()); out_s.reserve(tmp.len()); out_t.reserve(tmp.len()); out_r.reserve(tmp.len());
                for (site, ts) in tmp.into_iter() {
                    out_h.push(*h3_id); out_s.push(site); out_t.push(ts); out_r.push(r_val);
                }
            }
        }
        (out_h, out_s, out_t, out_r)
    });

    let h_arr = PyArray1::from_vec_bound(py, out_h);
    let s_arr = PyArray1::from_vec_bound(py, out_s);
    let t_arr = PyArray1::from_vec_bound(py, out_t);
    let r_arr = PyArray1::from_vec_bound(py, out_r);
    Ok((h_arr.into(), s_arr.into(), t_arr.into(), r_arr.into()))
}

/// Build CSR from raw arrays (node_ids/lats/lons and edges u/v/oneway and per-edge w_sec).
#[pyfunction]
fn build_csr_from_arrays(
    py: Python,
    node_ids: PyReadonlyArray1<i64>,
    lats: PyReadonlyArray1<f32>,
    lons: PyReadonlyArray1<f32>,
    edge_u: PyReadonlyArray1<i64>,
    edge_v: PyReadonlyArray1<i64>,
    oneway: PyReadonlyArray1<u8>,
    w_sec: PyReadonlyArray1<u16>,
) -> PyResult<(
    Py<PyArray1<i64>>, // node_ids (same order)
    Py<PyArray1<i64>>, // indptr
    Py<PyArray1<i32>>, // indices
    Py<PyArray1<u16>>, // w_sec (sorted)
    Py<PyArray1<f32>>, // lats
    Py<PyArray1<f32>>, // lons
)> {
    let node_ids = node_ids.as_slice()?;
    let lats = lats.as_slice()?;
    let lons = lons.as_slice()?;
    let u = edge_u.as_slice()?;
    let v = edge_v.as_slice()?;
    let oneway = oneway.as_slice()?;
    let w = w_sec.as_slice()?;
    if u.len() != v.len() || u.len() != oneway.len() || u.len() != w.len() {
        return Err(pyo3::exceptions::PyValueError::new_err("edge arrays must have the same length"));
    }
    let n_nodes = node_ids.len();

    // Heavy compute with GIL released
    let (indptr_vec, indices_vec, w_sorted) = py.allow_threads(|| {
        let mut map: FxHashMap<i64, i32> = FxHashMap::default();
        map.reserve(n_nodes * 2);
        for (i, &nid) in node_ids.iter().enumerate() { map.insert(nid, i as i32); }

        // Build directed edges
        let mut src: Vec<i32> = Vec::with_capacity(u.len() * 2);
        let mut dst: Vec<i32> = Vec::with_capacity(u.len() * 2);
        let mut wt: Vec<u16> = Vec::with_capacity(u.len() * 2);
        for i in 0..u.len() {
            let su = match map.get(&u[i]) { Some(&ix) => ix, None => continue };
            let sv = match map.get(&v[i]) { Some(&ix) => ix, None => continue };
            let wv = w[i];
            src.push(su); dst.push(sv); wt.push(wv);
            if oneway[i] == 0 { src.push(sv); dst.push(su); wt.push(wv); }
        }

        // Sort by (src, dst)
        let m = src.len();
        let mut order: Vec<usize> = (0..m).collect();
        order.sort_unstable_by(|&i, &j| {
            let a = (src[i], dst[i]);
            let b = (src[j], dst[j]);
            a.cmp(&b)
        });
        let mut src_s: Vec<i32> = Vec::with_capacity(m);
        let mut dst_s: Vec<i32> = Vec::with_capacity(m);
        let mut w_s: Vec<u16> = Vec::with_capacity(m);
        for &idx in &order { src_s.push(src[idx]); dst_s.push(dst[idx]); w_s.push(wt[idx]); }

        // Build indptr via counts
        let mut counts: Vec<i64> = vec![0; n_nodes];
        for &s in &src_s { counts[s as usize] += 1; }
        let mut indptr: Vec<i64> = vec![0; n_nodes + 1];
        for i in 0..n_nodes { indptr[i + 1] = indptr[i] + counts[i]; }

        (indptr, dst_s, w_s)
    });

    // Convert to numpy
    let node_ids_arr = PyArray1::from_vec_bound(py, node_ids.to_vec());
    let indptr_arr = PyArray1::from_vec_bound(py, indptr_vec);
    let indices_arr = PyArray1::from_vec_bound(py, indices_vec.into_iter().map(|x| x as i32).collect());
    let w_arr = PyArray1::from_vec_bound(py, w_sorted);
    let lats_arr = PyArray1::from_vec_bound(py, lats.to_vec());
    let lons_arr = PyArray1::from_vec_bound(py, lons.to_vec());
    Ok((node_ids_arr.into(), indptr_arr.into(), indices_arr.into(), w_arr.into(), lats_arr.into(), lons_arr.into()))
}

#[pymodule]
fn t_hex(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ch::CHGraph>()?;
    m.add_function(wrap_pyfunction!(kbest_multisource_csr, m)?)?;
    m.add_function(wrap_pyfunction!(kbest_multisource_bucket_csr, m)?)?;
    m.add_function(wrap_pyfunction!(aggregate_h3_topk, m)?)?;
    m.add_function(wrap_pyfunction!(aggregate_h3_topk_precached, m)?)?;
    m.add_function(wrap_pyfunction!(compute_h3_for_nodes, m)?)?;
    m.add_function(wrap_pyfunction!(weakly_connected_components, m)?)?;
    m.add_function(wrap_pyfunction!(build_csr_from_arrays, m)?)?;
    m.add_function(wrap_pyfunction!(ch::ch_build_from_csr, m)?)?;
    m.add_function(wrap_pyfunction!(ch::ch_from_bytes, m)?)?;
    Ok(())
}
