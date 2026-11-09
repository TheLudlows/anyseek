"""Compatibility path for the shared fresh-process resource wrapper."""
from pathlib import Path
import runpy

if __name__ == '__main__':
    runpy.run_path(str(Path(__file__).resolve().parents[1] / 'resource_run.py'), run_name='__main__')
