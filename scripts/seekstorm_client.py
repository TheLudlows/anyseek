#!/usr/bin/env python3
"""Reusable HTTP client for the seekstorm REST API, used by smoke tests.

Imported by scripts/index_write_query_smoke.py (and available to other test
scripts) so each scenario reads as a short sequence of method calls instead of
repeating urllib boilerplate. Requires only the Python standard library and a
running seekstorm_server; no published client SDK is needed.

Usage:
    client = SeekStormClient('http://127.0.0.1:54321')
    client.health()
    api_key = client.create_api_key(master_key)
    client.set_default_key(api_key)
    index_id = client.create_index({...})
    client.write_documents(index_id, [{'body': '...'}])
    client.commit(index_id)
    result = client.query(index_id, {'query': '...', 'search_mode': 'Lexical'})

Every non-2xx response raises AssertionError via the module-level check() helper,
matching the assertion style of scripts/http_smoke.py.
"""
import json
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

# Sentinel distinguishing "use the client's configured default key" (the common
# case) from "send no apikey header" (None, unauthenticated, e.g. health checks).
_DEFAULT = object()


def check(condition, message):
    if not condition:
        raise AssertionError(message)


class SeekStormClient:
    """Thin HTTP client for the seekstorm REST API surface used by smoke tests."""

    def __init__(self, base):
        self.base = base.rstrip('/')
        self._opener = build_opener(ProxyHandler({}))
        self._default_key = None

    def set_default_key(self, key):
        """Authenticate subsequent calls with this key unless one is passed."""
        self._default_key = key

    # -- low-level request helpers --------------------------------------

    def request(self, method, path, data=None, key=_DEFAULT, expected=200):
        if key is _DEFAULT:
            key = self._default_key
        headers = {}
        if key is not None:
            headers['apikey'] = key
        if data is not None and not isinstance(data, bytes):
            data = json.dumps(data, ensure_ascii=False).encode()
            headers['Content-Type'] = 'application/json'
        req = Request(self.base + path, data=data, headers=headers, method=method)
        try:
            response = self._opener.open(req, timeout=10)
        except HTTPError as error:
            response = error
        with response:
            body = response.read()
            check(response.status == expected,
                  f'{method} {path}: expected {expected}, got {response.status}: {body!r}')
            return body

    def api(self, method, path, data=None, key=_DEFAULT):
        value = json.loads(self.request(method, path, data, key))
        if isinstance(value, dict):
            check('Err' not in value, f'{method} {path}: {value}')
            if set(value) == {'Ok'}:
                return value['Ok']
        return value

    # -- public API surface ---------------------------------------------

    @staticmethod
    def index_path(index_id):
        return f'/api/v1/index/{index_id}'

    def health(self):
        """GET /api/v1/live - readiness probe, no authentication required."""
        self.request('GET', '/api/v1/live', key=None)

    def create_api_key(self, key):
        """POST /api/v1/apikey - mint a new API key (requires the master key)."""
        return self.request('POST', '/api/v1/apikey', {}, key).decode()

    def create_index(self, config, key=_DEFAULT):
        """POST /api/v1/index - create an index; returns the new index id."""
        return self.api('POST', '/api/v1/index', config, key)

    def write_documents(self, index_id, documents, key=_DEFAULT):
        """POST /api/v1/index/{id}/doc - index one document or a batch (uncommitted)."""
        self.api('POST', self.index_path(index_id) + '/doc', documents, key)

    def commit(self, index_id, key=_DEFAULT):
        """PATCH /api/v1/index/{id} - commit; returns the indexed document count."""
        return self.api('PATCH', self.index_path(index_id), None, key)

    def query(self, index_id, body, key=_DEFAULT):
        """POST /api/v1/index/{id}/query - search; returns the parsed result object."""
        return self.api('POST', self.index_path(index_id) + '/query', body, key)
