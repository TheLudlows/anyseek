#!/usr/bin/env python3
"""Keep standalone console assets out of the Rust service package."""
from pathlib import Path
import re

package = Path(__file__).resolve().parents[1] / 'anyseek'
assert not (package / 'web').exists(), 'frontend source belongs in console/, not the Rust package'
for source in (package / 'src').rglob('*.rs'):
    assert not re.search(r'include_(?:bytes|str)!\([^)]*(?:/web/|console/|dist/)', source.read_text()), f'embedded frontend in Rust: {source}'
print('PASS standalone console is absent from the Rust source/package boundary', flush=True)
