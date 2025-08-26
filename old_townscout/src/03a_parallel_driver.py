from multiprocessing import Pool, cpu_count
from src.config import STATES
from src.03_compute_minutes_per_state import compute_state

if __name__ == "__main__":
    with Pool(processes=max(1, cpu_count() - 1)) as p:
        p.map(compute_state, STATES) 