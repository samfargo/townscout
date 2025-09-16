use numpy::{PyArray2, PyReadonlyArray1, PyArrayMethods};
use pyo3::prelude::*;
use std::collections::BinaryHeap;
use std::cmp::Reverse;

const UNREACHABLE: u16 = 65535;

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

#[pymodule]
fn t_hex(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(kbest_multisource_csr, m)?)?;
    Ok(())
}
