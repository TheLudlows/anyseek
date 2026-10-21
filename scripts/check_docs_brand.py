#!/usr/bin/env python3
"""Check maintained local Markdown links and explicit brand exceptions (no network)."""
from pathlib import Path
import re
from urllib.parse import unquote

root = Path(__file__).resolve().parents[1]
docs = list(root.glob('*.md')) + list((root / 'docs').rglob('*.md')) + [root / 'anyseek_server/README.md']
errors = []
def anchors(path):
    result, counts, fenced = set(), {}, False
    for line in path.read_text().splitlines():
        if line.startswith('```'):
            fenced = not fenced
        if not fenced and line.startswith('#'):
            slug = re.sub(r'[^\w\-\s]', '', line.lstrip('#').strip().lower())
            slug = re.sub(r'\s', '-', slug)
            count = counts.get(slug, 0)
            counts[slug] = count + 1
            result.add(slug if not count else f'{slug}-{count}')
    return result

for path in docs:
    text = path.read_text()
    for match in re.finditer(r'\]\(([^\s\)]+)(?:\s+"[^"\n]*")?\)', text):
        target, _, fragment = match[1].partition('#')
        if target and not re.match(r'[a-z]+:', target) and not (path.parent / target).exists():
            errors.append(f'{path.relative_to(root)}: missing {target}')
        destination = path.parent / target if target else path
        if fragment and destination.is_file() and destination.suffix == '.md':
            if unquote(fragment) not in anchors(destination):
                errors.append(f'{path.relative_to(root)}: missing anchor {match[1]}')

# Current executable surfaces only; historical Markdown/third-party assets are not rebranded.
paths = list((root / 'anyseek/src').rglob('*.rs')) + list((root / 'anyseek_server/src').rglob('*.rs'))
paths += list((root / 'anyseek_server/web').rglob('*.js')) + list((root / 'anyseek_server/web').rglob('*.html')) + list((root / 'anyseek_server/web').rglob('*.svg'))
paths += list((root / 'scripts').glob('*.py')) + list((root / '.github/workflows').glob('*.yml'))
paths += [root / 'Dockerfile', root / 'Cargo.toml', root / 'anyseek/Cargo.toml', root / 'anyseek_server/Cargo.toml']
allowed = re.compile(r'https?://[^\s<>"\)]+|__seekpower_time_\w*|seekpower-feed-successor|\.seekpower-service-|seekpower\.(?:apiKey|index|pending\w*|serviceBackupId)')
for path in paths:
    if path.name == 'check_docs_brand.py':
        continue
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if re.search('seekpower', allowed.sub('', line), re.I):
            errors.append(f'{path.relative_to(root)}:{number}: unexpected legacy brand')
for error in errors:
    print(error)
if errors:
    raise SystemExit(1)
print(f'PASS {len(docs)} Markdown files: local files/heading links; {len(paths)} executable surfaces: explicit brand exceptions')
