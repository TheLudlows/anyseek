#!/usr/bin/env python3
"""Standard-library client for the Record HTTP contract.

Mutating calls require complete caller-retained requests. Persist them BEFORE
sending; reuse them after a timeout, busy response or unknown outcome. No method
allocates new identities or retries implicitly.
"""
import re
import json
import time
import uuid
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

_DEFAULT = object()


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def operation_id():
    """Allocate once for a NEW request, never inside a retry loop."""
    return {'epoch': int(time.time() // 86400), 'nonce': uuid.uuid4().hex}


class anyseekClient:
    def __init__(self, base):
        self.base = base.rstrip('/')
        self._opener = build_opener(ProxyHandler({}))
        self._default_key = None

    def set_default_key(self, key):
        self._default_key = key

    def request(self, method, path, data=None, key=_DEFAULT, expected=200):
        if key is _DEFAULT:
            key = self._default_key
        headers = {} if key is None else {'apikey': key}
        if data is not None:
            data = json.dumps(data, ensure_ascii=False).encode()
            headers['Content-Type'] = 'application/json'
        request = Request(self.base + path, data=data, headers=headers, method=method)
        try:
            response = self._opener.open(request, timeout=35)
        except HTTPError as error:
            response = error
        with response:
            body = response.read()
            check(response.status in (expected if isinstance(expected, tuple) else (expected,)),
                  f'{method} {path}: expected {expected}, got {response.status}: {body!r}')
            return body

    def api(self, method, path, data=None, key=_DEFAULT):
        return json.loads(self.request(method, path, data, key))

    @staticmethod
    def collection_path(collection_id):
        if not isinstance(collection_id,str) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,62}',collection_id):
            raise ValueError('collection_id must match ^[a-z][a-z0-9_-]{0,62}$')
        return '/api/v1/index/' + collection_id

    def health(self):
        return self.api('GET', '/api/v1/live', key=None)

    def create_tenant(self, request, key=_DEFAULT):
        """Master: {operation_id, tenant_id}; no rename or deletion."""
        return self.api('POST', '/api/v1/tenants', request, key)

    def register_key(self, request, key=_DEFAULT):
        """Master: {operation_id, tenant_id, key_id, apikey_base64}; client supplies key."""
        return self.api('POST', '/api/v1/apikey', request, key)

    def create_collection(self, request, key=_DEFAULT):
        """{operation_id, collection_id, config, embedding_space_id?}."""
        self.collection_path(request['collection_id'])
        return self.api('POST', '/api/v1/index', request, key)

    def write(self, collection_id, request, key=_DEFAULT):
        """{operation_id, record_id, routing_key, expected, document}; null deletes."""
        return self.api('POST', self.collection_path(collection_id) + '/doc', request, key)

    def batch(self, collection_id, request, key=_DEFAULT):
        """{writes:[RecordWrite,...]}; inspect each result, not an atomic transaction."""
        return self.api('POST', self.collection_path(collection_id) + '/batch', request, key)

    def ingest(self, collection_id, requests, key=_DEFAULT):
        """Return per-item results, including 429/503 envelopes; only receipts confirm success."""
        return json.loads(self.request('POST', self.collection_path(collection_id) + '/ingest',
                                       {'records': requests}, key, expected=(200, 429, 503)))

    def ingest_status(self, collection_id, operation, key=_DEFAULT):
        """Look up the canonical receipt; absence never proves a failed write."""
        path = self.collection_path(collection_id) + '/ingest/operation/'
        path += str(operation['epoch']) + '/' + quote(operation['nonce'], safe='')
        return self.api('GET', path, key=key)

    def ingest_progress(self, collection_id, key=_DEFAULT):
        """Read committed_seq, lexical_seq and vector_seq in Record sequence space."""
        return self.api('GET', self.collection_path(collection_id) + '/ingest', key=key)

    def checkpoint(self, collection_id, key=_DEFAULT):
        """Confirmed writes are already durable and visible."""
        return self.api('PATCH', self.collection_path(collection_id), key=key)

    def query(self, collection_id, request, key=_DEFAULT):
        """Returns hits[].record, visible_seq and optional count_total."""
        return self.api('POST', self.collection_path(collection_id) + '/query', request, key)
