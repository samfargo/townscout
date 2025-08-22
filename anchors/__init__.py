from .builder import AnchorBuilder
from .cli import main

__all__ = ["AnchorBuilder", "main"]

# -------------------------
# Anchors File structure
# -------------------------
# config.py — constants & defaults.
# networks.py — build/simplify graphs (drive/walk).
# core_utils.py — combined utilities: caching, H3 shims, geo helpers, I/O operations.
# candidates.py — generate drive/walk candidates (rural coverage, motorway chain, ped hubs, etc.).
# mandatories.py — bridgeheads, ferry, airports.
# selection.py — combined selection utilities: KD-tree building, snapping strategies, H3-based thinning.
# qa.py — acceptance checks.
# builder.py — orchestration.
# cli.py — argparse entrypoint (build_anchors.py equivalent).

