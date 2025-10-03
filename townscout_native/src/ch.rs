use std::cmp::Reverse;
use std::collections::BinaryHeap;

use fast_paths::{self, FastGraph32, InputGraph};
use numpy::{PyArray1, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

const INF_U32: u32 = u32::MAX;

#[pyclass(module = "t_hex")]
pub struct CHGraph {
    graph: FastGraph32,
    order: Vec<u32>,
    down_offsets: Vec<usize>,
    down_edges: Vec<(usize, u32)>,
}

impl CHGraph {
    fn from_fast_graph32(graph: FastGraph32) -> Self {
        let num_nodes = graph.ranks.len();
        let mut order = vec![0u32; num_nodes];
        for (node, &rank) in graph.ranks.iter().enumerate() {
            let r = rank as usize;
            if r < num_nodes {
                order[r] = node as u32;
            }
        }
        let mut counts = vec![0usize; num_nodes];
        for edge in &graph.edges_bwd {
            let adj = edge.adj_node as usize;
            if adj < num_nodes {
                counts[adj] += 1;
            }
        }
        let mut down_offsets = vec![0usize; num_nodes + 1];
        for i in 0..num_nodes {
            down_offsets[i + 1] = down_offsets[i] + counts[i];
        }
        let mut down_edges = vec![(0usize, 0u32); down_offsets[num_nodes]];
        let mut cursor = down_offsets.clone();
        for edge in &graph.edges_bwd {
            let adj = edge.adj_node as usize;
            let base = edge.base_node as usize;
            if adj >= num_nodes || base >= num_nodes {
                continue;
            }
            let pos = cursor[adj];
            down_edges[pos] = (base, edge.weight as u32);
            cursor[adj] = pos + 1;
        }
        Self {
            graph,
            order,
            down_offsets,
            down_edges,
        }
    }

    fn node_count(&self) -> usize {
        self.graph.ranks.len()
    }

    fn run_phast(&self, source: usize, limit: u32) -> Vec<u32> {
        let n = self.node_count();
        let mut dist = vec![INF_U32; n];
        if source >= n {
            return dist;
        }

        // Upward search (Dijkstra restricted to upward edges)
        let mut heap: BinaryHeap<(Reverse<u32>, usize)> = BinaryHeap::new();
        dist[source] = 0;
        heap.push((Reverse(0u32), source));

        while let Some((Reverse(du), u)) = heap.pop() {
            if du > limit {
                continue;
            }
            if du != dist[u] {
                continue;
            }
            let rank_u = self.graph.ranks[u] as usize;
            if rank_u >= self.graph.first_edge_ids_fwd.len() - 1 {
                continue;
            }
            let start = self.graph.first_edge_ids_fwd[rank_u] as usize;
            let end = self.graph.first_edge_ids_fwd[rank_u + 1] as usize;
            for idx in start..end {
                let edge = &self.graph.edges_fwd[idx];
                let v = edge.adj_node as usize;
                let w = edge.weight as u32;
                let nd = du.saturating_add(w);
                if nd > limit {
                    continue;
                }
                if nd < dist[v] {
                    dist[v] = nd;
                    heap.push((Reverse(nd), v));
                }
            }
        }

        // Downward sweep (PHAST)
        for &node_u in self.order.iter().rev() {
            let u = node_u as usize;
            let du = dist[u];
            if du == INF_U32 {
                continue;
            }
            let start = self.down_offsets[u];
            let end = self.down_offsets[u + 1];
            for idx in start..end {
                let (v, w) = self.down_edges[idx];
                let nd = du.saturating_add(w);
                if nd > limit {
                    continue;
                }
                if nd < dist[v] {
                    dist[v] = nd;
                }
            }
        }

        dist
    }
}

#[pymethods]
impl CHGraph {
    #[getter]
    fn num_nodes(&self) -> usize {
        self.node_count()
    }

    fn to_bytes(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        match bincode::serialize(&self.graph) {
            Ok(data) => Ok(PyBytes::new_bound(py, &data).unbind()),
            Err(e) => Err(PyValueError::new_err(format!("failed to serialize CH graph: {e}"))),
        }
    }

    #[pyo3(signature = (source, limit=None))]
    fn query_all(&self, py: Python<'_>, source: usize, limit: Option<u32>) -> PyResult<Py<PyArray1<u32>>> {
        let lim = limit.unwrap_or(u32::MAX);
        let dist = self.run_phast(source, lim);
        Ok(PyArray1::from_vec_bound(py, dist).unbind())
    }

    #[pyo3(signature = (source, targets, limit=None))]
    fn query_subset(
        &self,
        py: Python<'_>,
        source: usize,
        targets: PyReadonlyArray1<i32>,
        limit: Option<u32>,
    ) -> PyResult<Py<PyArray1<u32>>> {
        let lim = limit.unwrap_or(u32::MAX);
        let dist = self.run_phast(source, lim);
        let idx = targets.as_slice()?;
        let mut out = Vec::with_capacity(idx.len());
        for &raw in idx {
            if raw < 0 {
                out.push(INF_U32);
            } else {
                let j = raw as usize;
                if j < dist.len() {
                    out.push(dist[j]);
                } else {
                    out.push(INF_U32);
                }
            }
        }
        Ok(PyArray1::from_vec_bound(py, out).unbind())
    }

    fn debug_edges(&self, node: usize) -> PyResult<(usize, Vec<(usize, usize, u32)>, Vec<(usize, usize, u32)>)> {
        if node >= self.node_count() {
            return Err(PyValueError::new_err("node out of range"));
        }
        let rank = self.graph.ranks[node] as usize;
        let fwd_start = self.graph.first_edge_ids_fwd[rank] as usize;
        let fwd_end = self.graph.first_edge_ids_fwd[rank + 1] as usize;
        let mut fwd = Vec::new();
        for idx in fwd_start..fwd_end {
            let edge = &self.graph.edges_fwd[idx];
            fwd.push((edge.base_node as usize, edge.adj_node as usize, edge.weight as u32));
        }
        let bwd_start = self.down_offsets[node];
        let bwd_end = self.down_offsets[node + 1];
        let mut bwd = Vec::new();
        for idx in bwd_start..bwd_end {
            let (v, w) = self.down_edges[idx];
            bwd.push((node, v, w));
        }
        Ok((rank, fwd, bwd))
    }
}

fn build_input_graph(
    indptr: &[i64],
    indices: &[i32],
    w_sec: &[u16],
) -> Result<InputGraph, PyErr> {
    if indptr.is_empty() {
        return Err(PyValueError::new_err("indptr must be non-empty"));
    }
    if indices.len() != w_sec.len() {
        return Err(PyValueError::new_err("indices and weights must match in length"));
    }
    let mut g = InputGraph::new();
    let n = indptr.len() - 1;
    for u in 0..n {
        let lo = indptr[u] as usize;
        let hi = indptr[u + 1] as usize;
        if hi > indices.len() {
            return Err(PyValueError::new_err("indptr out of bounds for indices"));
        }
        for idx in lo..hi {
            let v_raw = indices[idx];
            if v_raw < 0 {
                continue;
            }
            let v = v_raw as usize;
            let w = w_sec[idx].max(1) as usize;
            g.add_edge(u, v, w);
        }
    }
    g.freeze();
    Ok(g)
}

#[pyfunction]
pub fn ch_build_from_csr(
    indptr: PyReadonlyArray1<i64>,
    indices: PyReadonlyArray1<i32>,
    w_sec: PyReadonlyArray1<u16>,
) -> PyResult<CHGraph> {
    let indptr = indptr.as_slice()?;
    let indices = indices.as_slice()?;
    let w_sec = w_sec.as_slice()?;

    let input = build_input_graph(indptr, indices, w_sec)?;
    let fast_graph = fast_paths::prepare(&input);
    let fast32 = FastGraph32::new(&fast_graph);
    Ok(CHGraph::from_fast_graph32(fast32))
}

#[pyfunction]
pub fn ch_from_bytes(data: &Bound<'_, PyBytes>) -> PyResult<CHGraph> {
    let bytes = data.as_bytes();
    match bincode::deserialize::<FastGraph32>(bytes) {
        Ok(graph) => Ok(CHGraph::from_fast_graph32(graph)),
        Err(e) => Err(PyValueError::new_err(format!("failed to deserialize CH graph: {e}"))),
    }
}
