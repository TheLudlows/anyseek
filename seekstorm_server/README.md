> SeekPower S1.3: file parsing and local dataset import have been removed. Prepare text, vectors and metadata externally, then send a JSON object or array to `POST /api/v1/index/{index_id}/doc`. PDF/CSV parsing, directory scanning, local JSON/SIFT import and PDF attachment hosting are no longer engine responsibilities. No PDFium installation is needed. Existing indexed records remain readable; legacy PDF files are not deleted, but the service no longer serves them. Host source files externally and put their URL in a record.

> SeekPower S1.2: model inference and automatic chunking have been removed. Supply external vectors through the HTTP API. The Web UI defaults to Lexical; Vector/Hybrid require a pasted JSON vector and display validation errors. See the [external vector contract](../README.md#external-vector-contract) for configuration, error handling and migration details.

# SeekStorm server 

SeekStorm open-source, multi-tenancy, vector & lexical search server

<img src="assets/logo.png" width="450" alt="Logo"><br>
[![Crates.io](https://img.shields.io/crates/v/seekstorm_server.svg)](https://crates.io/crates/seekstorm_server)
[![Downloads](https://img.shields.io/crates/d/seekstorm_server.svg?style=flat-square)](https://crates.io/crates/seekstorm_server)
[![Docker](https://img.shields.io/docker/pulls/wolfgarbe/seekstorm_server)](https://hub.docker.com/r/wolfgarbe/seekstorm_server)
[![REST API Documentation](https://docs.rs/seekstorm/badge.svg)](https://seekstorm.com/documentation/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/SeekStorm/SeekStorm?tab=Apache-2.0-1-ov-file#readme)
[![Roadmap](https://img.shields.io/badge/Roadmap-2026-DA7F07.svg)](#roadmap)

* The SeekStorm server is a standalone search server to be accessed via HTTP, while the SeekStorm crate/library can be embedded into your program.
* Index and search via [RESTful API](#rest-api-endpoints) endpoints and via [Embedded  web UI](#open-embedded-web-ui-in-browser).
* Multi-tenancy index management: multiple users, each with multiple indices
* API-key management
* Rate-limiting
* Cross-platform: runs on Linux, Windows, and macOS (other OS untested)

## Command line parameters

* index_path   (default = "/seekstorm_index" in the path of the current running executable)
* local_ip     (default = 0.0.0.0)
* local_port   (default = 80)
* force_shard_number : Set number of shards manually.
    - not set: number of shards is set automatically = number of physical processor cores (default)
    - small: slower indexing, higher latency, slightly higher throughput, faster realtime search, lower RAM consumption, more frequent auto-commit (docs round-robin distributed across shards, commit after 64k docs per shard)
    - large: faster indexing, lower latency, slightly lower throughput, slower realtime search, higher RAM consumption, less frequent auto-commit (docs round-robin distributed across shards, commit after 64k docs per shard)

```
./seekstorm_server.exe local_ip="127.0.0.1" local_port=80 index_path="c:/seekstorm_index"
```

## Console commands

Use the HTTP document API for single-record and batch writes. File import commands (`ingest`, `ingestcsv`, `ingestsift`, `searchsift`) have been removed, and `ingest_path` is rejected at startup. Retained commands: `create`, `delete`, `info`, `search`, `openapi`, `help`, `quit`.

```text
search -v [1,0,0]
```

`search` is a vector diagnostic for demo index 0 and requires an external-vector index.

The `create` command creates the demo API key. Create an index/schema through the HTTP API before writing records; see the examples below. The UI can display stored text and follow external source URLs, including PDF URLs, without an internal file endpoint.


```
delete
```
Delete the demo API key and all its indices.

```
quit
```
Exit server.

```
help
```
Show help.

## Manual file manipulation

While the server is not running, you may manually delete or backup API key and index directories or modify schema files in a text editor.  
In the index.json file you also may change access_type":"Mmap" to access_type":"Ram" and vice versa, as the index file format is identical.

## Open embedded Web UI in browser

The embedded web UI allows to search and display results (document fields) from an index:  
[http://127.0.0.1](http://127.0.0.1)

The Web UI uses the demo API key and index 0 by default. Create and populate the index through the JSON API first; select another index with the `index_id` URL parameter.

<img src="../../assets/wikipedia_demo.png" width="800">
<br><br>

To overwrite the default **API key** and **index_id** use url parameters: [http://127.0.0.1/?api_key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=&index_id=0](http://127.0.0.1/?api_key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=&index_id=0).  
This allows to open an web UI to multiple indices in different web browser tabs simultaneously.

If the default names defined in [master.js](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/web/js/master.js) for `TITLE_FIELD`, `TEXT_FIELD` and `URL_FIELD` do not exist in the schema of the selected index, then the first three fields (stored=true) from the index schema are automatically used for display in the web UI instead.

To permanently change the default **API key** or **index_id** of the embedded Web UI modify `API_KEY` and `INDEX_ID` in [/src/seekstorm_server/web/js/master.js](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/web/js/master.js) **before** building the seekstorm_server (html/css/js are embedded ressources).  
To permanently change the default **field names** from the index schema that are used in the web UI modify `TITLE_FIELD`, `TEXT_FIELD` and `URL_FIELD` [/src/seekstorm_server/web/js/master.js](https://github.com/SeekStorm/SeekStorm/blob/main/src/seekstorm_server/web/js/master.js) **before** building the seekstorm_server.

The embedded Web UI is intended for demonstration, test and debugging rather than for end customer use.

## REST API endpoints

See full [documentation of the REST API](https://seekstorm.com/documentation/).

You can use a REST client like [Bruno](https://www.usebruno.com/) or the VSC extension [Rest client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) to execute API calls, inspect responses and generate code snippets in your language:  
[**interactive API endpoint examples**](https://github.com/SeekStorm/SeekStorm/blob/master/src/seekstorm_server/test_api.rest)

&#x26A0; **CAUTION**: If sending CURL commands from MS Windows Powershell use 'curl.exe' instead of 'curl' AND escape (\") all double quotes within the JSON request object!


### create api key
Use the **master API key** displayed in the server console at startup.
WARNING: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.
```
curl --request POST --url http://127.0.0.1:80/api/v1/apikey --header 'apikey: BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=' --header 'content-type: application/json' --data '{"indices_max": 10,"indices_size_max":100000,"documents_max":10000000,"operations_max":10000000,"rate_limit": 100000}'
```
### delete api key
```
curl --request DELETE --url http://127.0.0.1/api/v1/apikey --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --header 'user-agent: vscode-restclient'
```

---

### create index
```
curl --request POST --url http://127.0.0.1:80/api/v1/index --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"schema":[{"field_type": "Text","store": true,"field": "title","index_lexical": true,"boost":10.0},{"field_type": "Text","store": true,"field": "body","index_lexical": true},{"field_type": "String","store": true,"field": "url","index_lexical": false}],"index_name": "test_index","similarity": "Bm25fProximity","tokenizer": "UnicodeAlphanumeric"}'
```
### get index
```
curl --request GET --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

### delete index
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

### commit index
```
curl --request PATCH --url http://127.0.0.1/api/v1/index/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```
---

### index document(s)

single document
```
curl --request POST --url http://127.0.0.1:80/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"title":"title1 test","body":"body1","url":"url1"}'
```

multiple documents
```
curl --request POST --url http://127.0.0.1:80/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --header 'user-agent: vscode-restclient' --data '[{"title":"title2","body":"body2 test","url":"url2"},{"title":"title3 test","body":"body3 test","url":"url3"}]'
```

### get document 

without highlight
```
curl --request GET --url http://127.0.0.1/api/v1/index/0/doc/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

with highlight
```
curl --request GET --url http://127.0.0.1/api/v1/index/0/doc/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query_terms": ["test"],"fields": ["title", "body"],"highlights": [{ "field": "title", "fragment_number": 0, "fragment_size": 1000, "highlight_markup": true},{ "field": "body", "fragment_number": 2, "fragment_size": 160, "highlight_markup": true},{ "field": "body", "name": "body2", "fragment_number": 0, "fragment_size": 4000, "highlight_markup": true}]}'
```
### update document(s) 

update document
```
curl --request PATCH --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '[0,{"title":"title1 test","body":"body1","url":"url1"}]'
```

update documents
```
curl --request PATCH --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '[[1,{"title":"title1 test","body":"body1","url":"url1"}],[2,{"title":"title3 test","body":"body3 test","url":"url3"}]]'
```

### delete document(s) 

delete document, by single document ID in URL parameter
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0/doc/0 --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

delete document, by single document ID in JSON request object
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data 0
```

delete documents, by vector of document IDs in JSON request object
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '[0,1]'
```

delete documents, by query in JSON request object
```
curl --request DELETE --url http://127.0.0.1/api/v1/index/0/doc --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query":"test","offset":0,"length":10,"realtime": true,"field_filter": ["title", "body"]}'
```

--- 

### query index (GET)

wit URL parameter
```
curl --request GET --url 'http://127.0.0.1/api/v1/index/0/query?query=test&offset=0&length=10' --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json'
```

with JSON parameter
```
curl --request GET --url http://127.0.0.1/api/v1/index/0/query --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query":"test","offset":0,"length":10,"realtime": true,"field_filter": ["title", "body"]}'
```

### query index (POST)

```
curl --request POST --url http://127.0.0.1/api/v1/index/0/query --header 'apikey: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' --header 'content-type: application/json' --data '{"query":"test","offset":0,"length":10,"realtime": true,"field_filter": ["title", "body"]}'
```

## Building

```
cargo build --release
```

&#x26A0; **WARNING**: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.
