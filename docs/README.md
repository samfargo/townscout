# TownScout Documentation

## Overview

This directory contains technical documentation for the TownScout interactive map system. Each document serves a specific purpose for developers working with the codebase.

## Document Structure

### 📖 Core Documentation

Architecture and implementation details now live in the root `README.md` (kept up to date alongside the code). See that file for:
- Data flow and pipeline overview
- Frontend-backend integration patterns
- Performance targets and checks
- Extension points for new features

**[DEBUGGING.md](DEBUGGING.md)** - Practical troubleshooting and diagnostics
- Quick diagnostic checklist for common issues
- Error patterns and their solutions
- Data flow tracing procedures
- Recovery and cleanup procedures

### 🗂️ Legacy Documentation

Historical notes have been folded into `README.md` and `docs/DEBUGGING.md`. Remove references to non-existent `ARCHITECTURE.md`.

## Quick Reference

### 🚨 Map Not Working?
1. Check `DEBUGGING.md` → "Quick Diagnostic Checklist"
2. Verify backend: `curl http://localhost:5174/health`
3. Check browser console for JavaScript errors

### 🏗️ Understanding the System?
1. Read the root `README.md` → "System Overview & Spec"
2. See the matrix factorization: `T_hex + D_anchor = total_travel_time`
3. Check the demo UI in `tiles/web/index.html`

### 🔧 Adding New Features?
1. Root `README.md` → "Implementation Tasks" and "Extension Points"
2. Follow the data pipeline: Data → Compute → Tiles → UI
3. Maintain the performance targets documented

## Key Concepts

- **T_hex**: Precomputed hex → nearest anchor travel times (PMTiles)
- **D_anchor**: Dynamic anchor → category travel times (API)
- **Filter Expression**: MapLibre GPU-accelerated client-side filtering
- **Matrix Factorization**: `total_time = hex_to_anchor + anchor_to_category`

The TownScout architecture achieves real-time interactivity by preprocessing heavy computations and using client-side GPU filtering for instant response to user input changes.
