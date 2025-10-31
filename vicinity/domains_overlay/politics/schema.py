"""
Politics data schema and constants.
"""

# Political lean buckets (0-4)
POLITICAL_LEAN_LABELS = {
    0: "Strong Democrat",
    1: "Lean Democrat", 
    2: "Moderate",
    3: "Lean Republican",
    4: "Strong Republican",
}

# Bucket thresholds based on Republican vote share
# 0.0-0.2 → 0 (Strong Democrat)
# 0.2-0.4 → 1 (Lean Democrat)
# 0.4-0.6 → 2 (Moderate)
# 0.6-0.8 → 3 (Lean Republican)
# 0.8-1.0 → 4 (Strong Republican)

def vote_share_to_bucket(rep_share: float) -> int:
    """
    Convert Republican vote share (0.0-1.0) to political lean bucket (0-4).
    
    Args:
        rep_share: Republican vote share as fraction (0.0 to 1.0)
        
    Returns:
        Political lean bucket: 0 (Strong Dem) to 4 (Strong Rep)
    """
    if rep_share < 0.2:
        return 0  # Strong Democrat
    elif rep_share < 0.4:
        return 1  # Lean Democrat
    elif rep_share < 0.6:
        return 2  # Moderate
    elif rep_share < 0.8:
        return 3  # Lean Republican
    else:
        return 4  # Strong Republican


# Output schema
# - h3_id (uint64)
# - res (int32)
# - political_lean (uint8): 0-4 bucket value
# - rep_vote_share (float32): Republican vote share 0.0-1.0
# - county_fips (str): FIPS code for the county
# - county_name (str): County name

