# TownScout Documentation

## Overview

This directory contains technical documentation for the TownScout interactive map system. Each document serves a specific purpose for developers working with the codebase.

## Document Structure

### 📖 Core Documentation

**[ARCHITECTURE.md](ARCHITECTURE.md)** - Complete system architecture and design decisions
- Data flow and pipeline overview
- Frontend-backend integration patterns  
- Performance optimizations and targets
- Extension points for new features

**[DEBUGGING.md](DEBUGGING.md)** - Practical troubleshooting and diagnostics
- Quick diagnostic checklist for common issues
- Error patterns and their solutions
- Data flow tracing procedures
- Recovery and cleanup procedures

### 🗂️ Legacy Documentation (Consolidated)

The following files contain historical development notes and have been consolidated into the main documents above:

- ~~`TROUBLESHOOTING.md`~~ → Merged into `DEBUGGING.md`
- ~~`FRONTEND_ARCHITECTURE.md`~~ → Merged into `ARCHITECTURE.md`  
- ~~`ARCHITECTURE_NOTES.md`~~ → Merged into `ARCHITECTURE.md`

## Quick Reference

### 🚨 Map Not Working?
1. Check `DEBUGGING.md` → "Quick Diagnostic Checklist"
2. Verify backend: `curl http://localhost:5174/health`
3. Check browser console for JavaScript errors

### 🏗️ Understanding the System?
1. Read `ARCHITECTURE.md` → "Data Flow Architecture"
2. See the matrix factorization: `T_hex + D_anchor = total_travel_time`
3. Check frontend filter expression implementation

### 🔧 Adding New Features?
1. `ARCHITECTURE.md` → "Extension Points"
2. Follow the data pipeline: Data → Compute → Tiles → UI
3. Maintain the performance targets documented

## Key Concepts

- **T_hex**: Precomputed hex → nearest anchor travel times (PMTiles)
- **D_anchor**: Dynamic anchor → category travel times (API)
- **Filter Expression**: MapLibre GPU-accelerated client-side filtering
- **Matrix Factorization**: `total_time = hex_to_anchor + anchor_to_category`

The TownScout architecture achieves real-time interactivity by preprocessing heavy computations and using client-side GPU filtering for instant response to user input changes.
