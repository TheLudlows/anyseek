# Rust 核心库示例

以下示例从原 README 迁移并修正已删除类型的引用，继续纳入 rustdoc 示例入口。请从本仓库使用 path/git 依赖；上游 crates.io 包不包含本分支修改。当前能力及限制见 [功能清单](../FEATURES.md)。

## Usage of the library

The single-record Rust API is `index_arc.index_document(document).await?`; the attachment `FileType` argument and `get_file` API have been removed. Use `index_documents` for batches and `get_document` for stored records.

### Lexical search

Add required crates to your project
```text
cargo add seekpower --path ../seekpower
cargo add tokio
cargo add serde_json
```

Use an asynchronous Rust runtime
```rust
use std::error::Error;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {

  // your Seekpower code here

   Ok(())
}
```

create schema (from JSON)
```rust
use seekpower::index::SchemaField;

let schema_json = r#"
[{"field":"title","field_type":"Text","store":false,"index_lexical":false},
{"field":"body","field_type":"Text","store":true,"index_lexical":true},
{"field":"url","field_type":"Text","store":false,"index_lexical":false}]"#;
let schema:Vec<SchemaField>=serde_json::from_str(schema_json).unwrap();
```

create schema (from SchemaField)
```rust
use seekpower::index::{SchemaField,FieldType};

let schema= vec![
    SchemaField::new("title".to_owned(), false, false,false, FieldType::Text, false,false, 1.0),
    SchemaField::new("body".to_owned(),true,true,false,FieldType::Text,false,true,1.0),
    SchemaField::new("url".to_owned(), false, false,false, FieldType::Text,false,false,1.0),
];
```

create index
```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use seekpower::index::{IndexMetaObject, Clustering, LexicalSimilarity,TokenizerType,StopwordType,FrequentwordType,AccessType,NgramSet,SchemaField,FieldType,DocumentCompression,create_index};
use seekpower::vector::Inference;
use seekpower::vector_similarity::VectorSimilarity;

let index_path=Path::new("C:/index/");

let schema= vec![
    SchemaField::new("title".to_owned(), false, false,false, FieldType::Text, false,false, 1.0),
    SchemaField::new("body".to_owned(),true,true,false,FieldType::Text,false,true,1.0),
    SchemaField::new("url".to_owned(), false, false, false,FieldType::Text,false,false,1.0),
];

let meta = IndexMetaObject {
    id: 0,
    name: "test_index".into(),
    lexical_similarity: LexicalSimilarity::Bm25f,
    tokenizer: TokenizerType::UnicodeAlphanumeric,
    stop_words: StopwordType::None,
    frequent_words: FrequentwordType::English,
    ngram_indexing: NgramSet::NgramFF as u8,
    document_compression: DocumentCompression::Snappy,
    access_type: AccessType::Mmap,
    clustering: Clustering::None,
    inference: Inference::None,
};

let segment_number_bits1=11;
let index_arc=create_index(index_path,meta,&schema, segment_number_bits1,false,None).await.unwrap();

# });
```

open index (alternatively to create index)
```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use seekpower::index::open_index;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

# });
```

index documents (from JSON)
```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use seekpower::index::{open_index, IndexDocuments};

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

let documents_json = r#"
[{"title":"title1 test","body":"body1","url":"url1"},
{"title":"title2","body":"body2 test","url":"url2"},
{"title":"title3 test","body":"body3 test","url":"url3"}]"#;
let documents_vec=serde_json::from_str(documents_json).unwrap();

index_arc.index_documents(documents_vec).await;

# });
```

index document (from Document)
```rust ,no_run
# tokio_test::block_on(async {

use seekpower::index::{Document, IndexDocument, open_index};
use std::path::Path;
use serde_json::Value;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

let document= Document::from([
    ("title".to_string(), Value::String("title4 test".to_string())),
    ("body".to_string(), Value::String("body4 test".to_string())),
    ("url".to_string(), Value::String("url4".to_string())),
]);

index_arc.index_document(document).await;

# });
```

commit documents
```rust ,no_run
# tokio_test::block_on(async {

use seekpower::commit::Commit;
use seekpower::index::open_index;
use std::path::Path;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

index_arc.commit().await;

# });
```

search index
```rust ,no_run
# tokio_test::block_on(async {

use seekpower::search::{Search, SearchMode, QueryType, ResultType};
use seekpower::index::open_index;
use std::path::Path;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

let query="test".to_string();
let query_vector=None;
let search_mode=SearchMode::Lexical;
let enable_empty_query=false;
let offset=0;
let length=10;
let query_type=QueryType::Intersection;
let result_type=ResultType::TopkCount;
let include_uncommitted=false;
let field_filter=Vec::new();
let facet_filter=Vec::new();
let result_sort=Vec::new();
let result_object = index_arc.search(query, query_vector, query_type, search_mode, enable_empty_query, offset, length, result_type,include_uncommitted,field_filter,facet_filter,result_sort).await;

// ### display results

use std::collections::HashSet;

let return_fields_filter= HashSet::new();
let mut index=index_arc.write().await;
for result in result_object.results.iter() {
  let doc=index.get_document(result.doc_id,false,&return_fields_filter).await.unwrap();
  println!("result {} rank {} body field {:?}" , result.doc_id,result.score, doc.get("body"));
}

println!("result counts {} {} {}",result_object.results.len(), result_object.result_count, result_object.result_count_total);

# })
```

*Query operators and query type*

Boolean queries are specified in the search method either via the query_type parameter or via operator chars within the query parameter.
The interpretation of operator chars within the query string (set `query_type=QueryType::Union`) allows to specify advanced search operations via a simple search box.

Intersection, AND `+`
```rust ,no_run
use seekpower::search::QueryType;
let query_type=QueryType::Union;
let query="+red +apple".to_string();
```

```rust ,no_run
use seekpower::search::QueryType;
let query_type=QueryType::Intersection;
let query="red apple".to_string();
```

Union, OR
```rust ,no_run
use seekpower::search::QueryType;
let query_type=QueryType::Union;
let query="red apple".to_string();
```

Phrase `""`
```rust ,no_run
use seekpower::search::QueryType;
let query_type=QueryType::Union;
let query="\"red apple\"".to_string();
```

```rust ,no_run
use seekpower::search::QueryType;
let query_type=QueryType::Phrase;
let query="red apple".to_string();
```

Except, minus, NOT `-`
```rust ,no_run
use seekpower::search::QueryType;
let query_type=QueryType::Union;
let query="apple -red".to_string();
```

Mixed phrase and intersection
```rust ,no_run
use seekpower::search::QueryType;
let query_type=QueryType::Union;
let query="+\"the who\" +uk".to_string();
```


multi-threaded search
```rust ,no_run
# tokio_test::block_on(async {

use seekpower::search::{QueryType, SearchMode, ResultType, Search};
use std::sync::Arc;
use tokio::sync::Semaphore;
use seekpower::index::open_index;
use std::path::Path;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

let query_vec=vec!["house".to_string(),"car".to_string(),"bird".to_string(),"sky".to_string()];
let query_vector=None;
let offset=0;
let length=10;
let search_mode=SearchMode::Lexical;
let enable_empty_query=false;
let query_type=QueryType::Union;
let result_type=ResultType::TopkCount;

let include_uncommitted=false;
let field_filter=Vec::new();
let facet_filter=Vec::new();
let result_sort=Vec::new();

let thread_number = 4;
let permits = Arc::new(Semaphore::new(thread_number));
for query in query_vec {
    let permit_thread = permits.clone().acquire_owned().await.unwrap();

    let query_clone = query.clone();
    let query_vector_clone=query_vector.clone();
    let index_arc_clone = index_arc.clone();
    let search_mode_clone=search_mode.clone();
    let enable_empty_query_clone=enable_empty_query.clone();
    let offset_clone = offset;
    let length_clone = length;
    let query_type_clone = query_type.clone();
    let result_type_clone = result_type.clone();
    let include_uncommitted_clone=include_uncommitted;
    let field_filter_clone=field_filter.clone();
    let facet_filter_clone=facet_filter.clone();
    let result_sort_clone=result_sort.clone();

    tokio::spawn(async move {
        let rlo = index_arc_clone
            .search(
                query_clone,
                query_vector_clone,
                query_type_clone,
                search_mode_clone,
                enable_empty_query_clone,
                offset_clone,
                length_clone,
                result_type_clone,
                include_uncommitted_clone,
                field_filter_clone,

                facet_filter_clone,
                result_sort_clone,)
            .await;

        println!("result count {}", rlo.result_count);

        drop(permit_thread);
    });
}

# })
```

clear index
```rust, no_run
# tokio_test::block_on(async {
use seekpower::index::open_index;
use std::path::Path;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

index_arc.write().await.clear_index().await;

# });
```

delete index
```rust ,no_run
# tokio_test::block_on(async {

use seekpower::index::open_index;
use std::path::Path;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

index_arc.write().await.delete_index();

# });
```


iterate through document ID of an index
```rust ,no_run
# tokio_test::block_on(async {

use seekpower::{index::open_index,iterator::GetIterator};
use std::path::Path;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

//display min_docid: the min_docid is NOT always 0, if the first shards are empty!
let iterator=index_arc.get_iterator(None,0,1,false,false,vec![]).await;
println!("min doc_id: {}",iterator.results.first().unwrap().doc_id);

//display max_docid
let iterator=index_arc.get_iterator(None,0,-1,false,false,vec![]).await;
println!("max doc_id: {}",iterator.results.first().unwrap().doc_id);

//iterate doc_id ascending, display the lowest 10 and then every 10_000th document ID
let mut iterator=index_arc.get_iterator(None,0,1,false,false,vec![]).await;
let mut i=0;
if !iterator.results.is_empty() {println!("$ i: {} doc_id: {}",i,iterator.results.first().unwrap().doc_id);}
while !iterator.results.is_empty() {
    iterator=index_arc.get_iterator(Some(iterator.results.first().unwrap().doc_id),1,1,false,false,vec![]).await;
    i+=1;
    if !iterator.results.is_empty() && ( i % 10_000 ==0 || i<=10 )  {println!("i: {} doc_id: {}",i,iterator.results.first().unwrap().doc_id);}
}

//iterate doc_id descending, display the highest 10 and then every 10_000th document ID
let mut iterator=index_arc.get_iterator(None,0,-1,false,false,vec![]).await;
let mut i=0;
if !iterator.results.is_empty() {println!("$ i: {} doc_id: {}",i,iterator.results.first().unwrap().doc_id);}
while !iterator.results.is_empty() {
    iterator=index_arc.get_iterator(Some(iterator.results.first().unwrap().doc_id),1,-1,false,false,vec![]).await;
    i+=1;
    if !iterator.results.is_empty() && ( i % 10_000 ==0 || i<=10 )  {println!("i: {} doc_id: {}",i,iterator.results.first().unwrap().doc_id);}
}

index_arc.write().await.delete_index();

# });
```


close index
```rust ,no_run
# tokio_test::block_on(async {

use seekpower::index::open_index;
use seekpower::index::Close;
use std::path::Path;

let index_path=Path::new("C:/index/");
let mut index_arc=open_index(index_path).await.unwrap();

index_arc.close().await;

# });
```

seekpower library version string
```rust ,no_run
use seekpower::index::version;

let version=version();
println!("version {}",version);
```
<br/>

---
### Metadata fields - Quick start

Metadata filtering uses schema fields and query filters:
1. The facet fields are defined in the schema at create_index.
2. The facet field values are set in index_document at index time.
3. Use facet_filter at query time. Aggregate results are no longer returned.

This example combines metadata indexing, filtering, and search:

Add required crates to your project
```text
cargo add seekpower --path ../seekpower
cargo add tokio
cargo add serde_json
```

Use an asynchronous Rust runtime
```rust ,no_run
use std::error::Error;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {

  // your Seekpower code here

   Ok(())
}
```
create index
```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use std::sync::{Arc, RwLock};
use seekpower::index::{IndexMetaObject, Clustering,LexicalSimilarity,TokenizerType,StopwordType,FrequentwordType,AccessType,NgramSet,DocumentCompression,create_index};
use seekpower::vector::Inference;
use seekpower::vector_similarity::VectorSimilarity;

let index_path=Path::new("C:/index/");//x

let schema_json = r#"
[{"field":"title","field_type":"Text","store":false,"index_lexical":false},
{"field":"body","field_type":"Text","store":true,"index_lexical":true},
{"field":"url","field_type":"Text","store":true,"index_lexical":false},
{"field":"town","field_type":"String16","store":false,"index_lexical":false,"facet":true}]"#;
let schema=serde_json::from_str(schema_json).unwrap();

let meta = IndexMetaObject {
    id: 0,
    name: "test_index".into(),
    lexical_similarity: LexicalSimilarity::Bm25f,
    tokenizer: TokenizerType::AsciiAlphabetic,
    stop_words: StopwordType::None,
    frequent_words: FrequentwordType::English,
    ngram_indexing: NgramSet::NgramFF as u8,
    document_compression: DocumentCompression::Snappy,
    access_type: AccessType::Mmap,
    clustering: Clustering::None,
    inference: Inference::None,
};


let segment_number_bits1=11;
let index_arc=create_index(index_path,meta,&schema, segment_number_bits1,false,None).await.unwrap();

# });
```

index documents
```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use seekpower::index::{IndexDocuments,open_index};
use seekpower::commit::Commit;

let index_path=Path::new("C:/index/");
let index_arc=open_index(index_path).await.unwrap();

let documents_json = r#"
[{"title":"title1 test","body":"body1","url":"url1","town":"Berlin"},
{"title":"title2","body":"body2 test","url":"url2","town":"Warsaw"},
{"title":"title3 test","body":"body3 test","url":"url3","town":"New York"}]"#;
let documents_vec=serde_json::from_str(documents_json).unwrap();

index_arc.index_documents(documents_vec).await;

// ### commit documents

index_arc.commit().await;

# });
```

search index
```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use seekpower::index::{IndexDocuments,open_index};
use seekpower::search::{Search,SearchMode,QueryType,ResultType};
use std::collections::HashSet;

let index_path=Path::new("C:/index/");
let index_arc=open_index(index_path).await.unwrap();
let query="test".to_string();
let query_vector=None;
let search_mode=SearchMode::Lexical;
let enable_empty_query=false;
let offset=0;
let length=10;
let query_type=QueryType::Intersection;
let result_type=ResultType::TopkCount;
let include_uncommitted=false;
let field_filter=Vec::new();
let facet_filter=Vec::new();
//let facet_filter = vec![FacetFilter::String16 { field: "town".to_string(),filter: vec!["Berlin".to_string()],}];
let result_sort=Vec::new();

let result_object = index_arc.search(query, query_vector, query_type, search_mode, enable_empty_query, offset, length, result_type,include_uncommitted,field_filter,facet_filter,result_sort,).await;

// ### display results

let return_fields_filter= HashSet::new();
let index=index_arc.write().await;
for result in result_object.results.iter() {
  let doc=index.get_document(result.doc_id,false,&return_fields_filter).await.unwrap();
  println!("result {} rank {} body field {:?}" , result.doc_id,result.score, doc.get("body"));
}
println!("result counts {} {} {}",result_object.results.len(), result_object.result_count, result_object.result_count_total);

// Stored document results only


# });
```

### Vector search: external inference

create index
```rust ,no_run
# tokio_test::block_on(async {

    use std::path::Path;
    use std::sync::{Arc, RwLock};
    use seekpower::index::{IndexMetaObject, Clustering,LexicalSimilarity,TokenizerType,StopwordType,FrequentwordType,AccessType,NgramSet,DocumentCompression,create_index};
    use seekpower::vector::{Embedding, Inference, Precision, Quantization};
    use seekpower::vector_similarity::VectorSimilarity;

   let index_path=Path::new("tests/index_test/");

    let schema_json = r#"
    [{"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true},
    {"field":"index","field_type":"Text","store":true,"index_lexical":false,"index_vector":false}]"#;
    let schema=serde_json::from_str(schema_json).unwrap();

    let meta = IndexMetaObject {
        id: 0,
        name: "test_index".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stop_words: StopwordType::None ,
        frequent_words: FrequentwordType::English,
        ngram_indexing: NgramSet::SingleTerm as u8 ,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::External { dimensions: 128, precision: Precision::F32,  quantization: Quantization::None,similarity:VectorSimilarity::Euclidean },
    };

    let segment_number_bits1=11;
    let index_arc=create_index(index_path,meta,&schema, segment_number_bits1,false,None).await.unwrap();
    let index=index_arc.read().await;

    let result=index.meta.id;
    assert_eq!(result, 0);

# });
```

index documents/vectors
```rust ,no_run
# tokio_test::block_on(async {

    use std::path::Path;
    use seekpower::index::{IndexDocuments,open_index};
    use seekpower::commit::Commit;

    // open index
    let index_path=Path::new("tests/index_test/");
    let index_arc=open_index(index_path).await.unwrap();

    // index documents
    let documents_json = r#"
    [{"vector":[0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010, 0.011, 0.012, 0.013, 0.014, 0.015, 0.016, 0.017, 0.018, 0.019, 0.020, 0.021, 0.022, 0.023, 0.024, 0.025, 0.026, 0.027, 0.028, 0.029, 0.030, 0.031, 0.032, 0.033, 0.034, 0.035, 0.036, 0.037, 0.038, 0.039, 0.040, 0.041, 0.042, 0.043, 0.044, 0.045, 0.046, 0.047, 0.048, 0.049, 0.050, 0.051, 0.052, 0.053, 0.054, 0.055, 0.056, 0.057, 0.058, 0.059, 0.060, 0.061, 0.062, 0.063, 0.064, 0.065, 0.066, 0.067, 0.068, 0.069, 0.070, 0.071, 0.072, 0.073, 0.074, 0.075, 0.076, 0.077, 0.078, 0.079, 0.080, 0.081, 0.082, 0.083, 0.084, 0.085, 0.086, 0.087, 0.088, 0.089, 0.090, 0.091, 0.092, 0.093, 0.094, 0.095, 0.096, 0.097, 0.098, 0.099, 0.100, 0.101, 0.102, 0.103, 0.104, 0.105, 0.106, 0.107, 0.108, 0.109, 0.110, 0.111, 0.112, 0.113, 0.114, 0.115, 0.116, 0.117, 0.118, 0.119, 0.120, 0.121, 0.122, 0.123, 0.124, 0.125, 0.126, 0.127, 0.128],"index":"0"},
    {"vector":[0.129, 0.130, 0.131, 0.132, 0.133, 0.134, 0.135, 0.136, 0.137, 0.138, 0.139, 0.140, 0.141, 0.142, 0.143, 0.144, 0.145, 0.146, 0.147, 0.148, 0.149, 0.150, 0.151, 0.152, 0.153, 0.154, 0.155, 0.156, 0.157, 0.158, 0.159, 0.160, 0.161, 0.162, 0.163, 0.164, 0.165, 0.166, 0.167, 0.168, 0.169, 0.170, 0.171, 0.172, 0.173, 0.174, 0.175, 0.176, 0.177, 0.178, 0.179, 0.180, 0.181, 0.182, 0.183, 0.184, 0.185, 0.186, 0.187, 0.188, 0.189, 0.190, 0.191, 0.192, 0.193, 0.194, 0.195, 0.196, 0.197, 0.198, 0.199, 0.200, 0.201, 0.202, 0.203, 0.204, 0.205, 0.206, 0.207, 0.208, 0.209, 0.210, 0.211, 0.212, 0.213, 0.214, 0.215, 0.216, 0.217, 0.218, 0.219, 0.220, 0.221, 0.222, 0.223, 0.224, 0.225, 0.226, 0.227, 0.228, 0.229, 0.230, 0.231, 0.232, 0.233, 0.234, 0.235, 0.236, 0.237, 0.238, 0.239, 0.240, 0.241, 0.242, 0.243, 0.244, 0.245, 0.246, 0.247, 0.248, 0.249, 0.250, 0.251, 0.252, 0.253, 0.254, 0.255, 0.256],"index":"1"},
    {"vector":[0.257, 0.258, 0.259, 0.260, 0.261, 0.262, 0.263, 0.264, 0.265, 0.266, 0.267, 0.268, 0.269, 0.270, 0.271, 0.272, 0.273, 0.274, 0.275, 0.276, 0.277, 0.278, 0.279, 0.280, 0.281, 0.282, 0.283, 0.284, 0.285, 0.286, 0.287, 0.288, 0.289, 0.290, 0.291, 0.292, 0.293, 0.294, 0.295, 0.296, 0.297, 0.298, 0.299, 0.300, 0.301, 0.302, 0.303, 0.304, 0.305, 0.306, 0.307, 0.308, 0.309, 0.310, 0.311, 0.312, 0.313, 0.314, 0.315, 0.316, 0.317, 0.318, 0.319, 0.320, 0.321, 0.322, 0.323, 0.324, 0.325, 0.326, 0.327, 0.328, 0.329, 0.330, 0.331, 0.332, 0.333, 0.334, 0.335, 0.336, 0.337, 0.338, 0.339, 0.340, 0.341, 0.342, 0.343, 0.344, 0.345, 0.346, 0.347, 0.348, 0.349, 0.350, 0.351, 0.352, 0.353, 0.354, 0.355, 0.356, 0.357, 0.358, 0.359, 0.360, 0.361, 0.362, 0.363, 0.364, 0.365, 0.366, 0.367, 0.368, 0.369, 0.370, 0.371, 0.372, 0.373, 0.374, 0.375, 0.376, 0.377, 0.378, 0.379, 0.380, 0.381, 0.382, 0.383, 0.384],"index":"2"}]"#;
    let documents_vec=serde_json::from_str(documents_json).unwrap();
    index_arc.index_documents(documents_vec).await;

    // wait until all index threads are finished and commit
    index_arc.commit().await;

    let result=index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 3);

# });
```

query documents/vectors
```rust ,no_run
# tokio_test::block_on(async {

    use std::path::Path;
    use seekpower::index::{IndexDocuments,open_index};
    use seekpower::search::{Search,SearchMode,QueryType,ResultType};
    use seekpower::vector_similarity::{AnnMode, VectorSimilarity};
    use seekpower::commit::Commit;
    use seekpower::vector::Embedding;
    use std::collections::HashSet;

  // open index
    let index_path=Path::new("tests/index_test/");
    let index_arc=open_index(index_path).await.unwrap();

    let result=index_arc.read().await.indexed_doc_count().await;
    assert_eq!(result, 3);

    let query=String::new();
    let query_vector = vec![0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010, 0.011, 0.012, 0.013, 0.014, 0.015, 0.016, 0.017, 0.018, 0.019, 0.020, 0.021, 0.022, 0.023, 0.024, 0.025, 0.026, 0.027, 0.028, 0.029, 0.030, 0.031, 0.032, 0.033, 0.034, 0.035, 0.036, 0.037, 0.038, 0.039, 0.040, 0.041, 0.042, 0.043, 0.044, 0.045, 0.046, 0.047, 0.048, 0.049, 0.050, 0.051, 0.052, 0.053, 0.054, 0.055, 0.056, 0.057, 0.058, 0.059, 0.060, 0.061, 0.062, 0.063, 0.064, 0.065, 0.066, 0.067, 0.068, 0.069, 0.070, 0.071, 0.072, 0.073, 0.074, 0.075, 0.076, 0.077, 0.078, 0.079, 0.080, 0.081, 0.082, 0.083, 0.084, 0.085, 0.086, 0.087, 0.088, 0.089, 0.090, 0.091, 0.092, 0.093, 0.094, 0.095, 0.096, 0.097, 0.098, 0.099, 0.100, 0.101, 0.102, 0.103, 0.104, 0.105, 0.106, 0.107, 0.108, 0.109, 0.110, 0.111, 0.112, 0.113, 0.114, 0.115, 0.116, 0.117, 0.118, 0.119, 0.120, 0.121, 0.122, 0.123, 0.124, 0.125, 0.126, 0.127, 0.128];
    let query_embedding=Embedding::F32(query_vector);
    let result_object = index_arc
        .search(
            query,
            Some(query_embedding),
            QueryType::Union,
            SearchMode::Vector { similarity_threshold: None, ann_mode: AnnMode::All },
            false,
            0,
            10,
            ResultType::TopkCount,
            false,
            Vec::new(),

            Vec::new(),
            Vec::new(),
        )
        .await;

    let result=result_object.results.len();
    assert_eq!(result, 3);

    let result=result_object.result_count;
    assert_eq!(result, 3);

    let result=result_object.result_count_total;
    assert_eq!(result, 3);

# });
```

