# 分面、过滤与排序

当前词法路径保留分面统计、元数据过滤和字段排序。它们共用 schema 中的 `facet:true` 字段：`query_facets` 请求统计，`facet_filter` 限制结果，`result_sort` 设定排序。`field_filter` 仅选择检索字段，含义不同。

| 类型 | 能力 |
| --- | --- |
| String16/String32 | 按字符串过滤、计数、前缀限制和排序 |
| StringSet16/StringSet32 | 多值字符串集合过滤与统计 |
| 整数、浮点、Timestamp | 范围过滤、分桶计数、CountWithinRange/CountAboveRange/CountBelowRange、排序 |
| 索引信息 | 返回已配置 facet 的信息及数值 min/max |

统计行为与 `ResultType` 有关，完整分面计数应使用 Count/TopkCount。Vector/Hybrid 非空 `facet_filter` 返回错误；Hybrid 的分面来自词法分支，不是融合结果的聚合。Vector/Hybrid 可按字段重排已召回候选，但向量候选已先按相似度截断，因此不保证全库字段排序 Top-K。

下面保留 Rust 接口示例；本地使用 `cargo doc` 生成当前源码文档，HTTP 使用 POST JSON，参见 [服务文档](seekstorm_server/README.md) 和 [功能清单](FEATURES.md)。

### Relevant API endpoints

#### Create index

Facet fields allow counting, filtering, and sorting of indexed documents matching specific **facet field values** or **numeric ranges**.

**Create index**: Facet fields are defined in the **schema** parameter of **create_index** by setting the **field_type** to one of the following types: 

```text
U8, U16, U32, U64, I8, I16, I32, I64, F32, F64, Timestamp, String16, String32, StringSet16, StringSet32
```
and set
```text
"facet":true
```
in the schema (see code below).

Faceting can be enabled both for string field type and numerical field types.
Both numerical, string and text fields can be indexed (index_lexical=true) and stored (store=true) in the json document,
but with facet=true a numerical or string field is additionally stored in a binary format, for fast faceting and sorting without document store access (decompression, deserialization).
Setting a numerical field to "facet":true is also the precondition for sorting the search result by this fields value ('score' is treated as a special field to allow sorting by relevancy).

```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use std::sync::{Arc, RwLock};
use seekstorm::index::{IndexMetaObject, Clustering,LexicalSimilarity,TokenizerType,StopwordType,FrequentwordType,AccessType,StemmerType,NgramSet,DocumentCompression,create_index};
use seekstorm::vector::Inference;
use seekstorm::vector_similarity::VectorSimilarity;

let index_path=Path::new("C:/index/");//x

let schema_json = r#"
[{"field":"title","field_type":"Text","store":false,"index_lexical":false},
{"field":"body","field_type":"Text","store":true,"index_lexical":true},
{"field":"url","field_type":"Text","store":true,"index_lexical":false},
{"field":"town","field_type":"String16","store":false,"index_lexical":false,"facet":true}]"#;
let schema=serde_json::from_str(schema_json).unwrap();

let meta = IndexMetaObject {
    id: 0,
    name: "test_index".to_string(),
    lexical_similarity: LexicalSimilarity::Bm25f,
    tokenizer: TokenizerType::AsciiAlphabetic,
    stemmer: StemmerType::None,
    stop_words: StopwordType::None,
    frequent_words: FrequentwordType::English,
    ngram_indexing: NgramSet::NgramFF as u8,
    document_compression: DocumentCompression::Snappy,
    access_type: AccessType::Mmap,
    clustering: Clustering::None,
    inference: Inference::None,
};

let synonyms=Vec::new();

let segment_number_bits1=11;
let index_arc=create_index(index_path,meta,&schema,&synonyms,segment_number_bits1,false,None).await.unwrap();

# });
```

#### Index document(s)

While the **type** of the facet fields (U8, U16, U32, U64, I8, I16, I32, I64, F32, F64, Timestamp, String16, String32, StringSet16, StringSet32) is defined in **create index**, the **value** of those fields
is defined and indexed with **index document**.

**Index document request object**: Each facet field can be assigned a single string values or numerical value per document:<br>

```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use seekstorm::index::{IndexDocuments,open_index};
use seekstorm::commit::Commit;

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

In addition to the facet functionality (counting, filtering, sorting) the facet value can be also indexed to full-text search.<br>
The number of distinct values per string facet field, across the whole index, is limited to 65_535 (String16) and to 4_294_967_295 (String32).<br>
The number of distinct value combinations per string set facet field, across the whole index, is limited to 65_535 (StringSet16) and to 4_294_967_295 (StringSet32).<br>
The string length of a stringFacet value is limited to 100 characters.


#### Get index string facets

**get_index_string_facets**: list of string facet fields, each with field name and a map of unique values and their count (number of times the specific value appears in the whole index).<br>
Values are sorted by their occurrence count within all indexed documents in descending order.<br> 
The string facets returned with get_index_string_facets are defined and created at index time, not at query time.<br> 
They contain the facet values of the whole index, not only the documents matching a specific query.<br> 
Therefore only string facets can be returned with get_index_string_facets, while numerical range facets are defined at query time and can't be generated at index time.


```rust ,no_run
# tokio_test::block_on(async {

use std::path::Path;
use seekstorm::search::QueryFacet;
use seekstorm::index::{IndexArc,open_index};

let index_path=Path::new("C:/index/");
let index_arc=open_index(index_path).await.unwrap();

let query_facets = vec![
    QueryFacet::String16 {
        field: "age".into(),
        prefix: "".into(),
        length: u16::MAX},
];

let string_facets=index_arc.read().await.get_index_string_facets(query_facets).await.unwrap();
println!("{}", serde_json::to_string_pretty(&string_facets).unwrap());

# });
```

**query_facets filter**: If there are many distinct values per facet field, we might want to filter the returned values to those matching a given prefix or limiting the number of returned distinct values.

The **prefix** property of a QueryFacet allows to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.<br>
The **length** property of a QueryFacet allows limiting the number of returned distinct values per facet field, if there are too many distinct values. With length=0 no facts are returned at all.<br>
The values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.

```rust
use seekstorm::search::QueryFacet;

let query_facets = vec![
    QueryFacet::String16 {
        field: "language".into(),
        prefix: "ger".into(),
        length: 10},
];
```

**get_index_string_facets** returns a **vector of index facets**. For all facet fields, it is counted how often each distinct value occurs **within all indexed documents**.<br>
Within each facet field, the values are sorted by their occurrence count **within all indexed documents** in descending order.

```json
[
  {
    "field": "fruit",
    "values": [
      [
        "Apple",
        41912
      ],
      [
        "Pear",
        41863
      ],
      [
        "Cherry",
        41524
      ],
      [
        "Orange",
        41516
      ],
      [
        "Plum",
        41502
      ]
    ]
  }
]
```

**Range facets** for numerical values are returned with **query documents**, 
where the ranges can be defined dynamically with different boundaries for each query. This allows defining facets ranges like Last hour, last day, last week, last month.<br>


#### Search index

```rust ,no_run
# tokio_test::block_on(async {

use seekstorm::search::{Search, SearchMode, QueryType, ResultType, QueryFacet};
use seekstorm::index::open_index;
use std::path::Path;
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
let query_facets = vec![QueryFacet::String16 {field: "age".into(),prefix: "".into(),length:u16::MAX}];
let facet_filter=Vec::new();
let result_sort=Vec::new();

let result_object = index_arc.search(query, query_vector, query_type, search_mode, enable_empty_query, offset, length, result_type,include_uncommitted,field_filter,query_facets,facet_filter,result_sort,).await;


// **display results**

let return_fields_filter= HashSet::new();
let index=index_arc.read().await;
for result in result_object.results.iter() {
  let doc=index.get_document(result.doc_id,false,&return_fields_filter).await.unwrap();
  println!("result {} rank {} body field {:?}" , result.doc_id,result.score, doc.get("body"));
}
println!("result counts {} {} {}",result_object.results.len(), result_object.result_count, result_object.result_count_total);

// **display facets**

println!("{}", serde_json::to_string_pretty(&result_object.facets).unwrap());

# });
```

**Query**: The query defines which of the indexed documents are returned  as search results.
```text
  query=test
```


**query_facets**: The query_facets parameter specifies for both for string facets and numerical range facets which facet field to return at query time.

**String facets** allow to filter the returned string facet values by prefix and maximum number.
If there are many distinct values per facet field, we might want to filter the returned values to those matching a given prefix or limiting the number of returned distinct values.

The **prefix** property of a QueryFacet allows to filter the returned facet values to those matching a given prefix, if there are too many distinct values per facet field.<br>
The **length** property of a QueryFacet allows limiting the number of returned distinct values per facet field, if there are too many distinct values. With length=0 no facts are returned at all.<br>
With length=0 no query facets are returned, but facet filtering can still be used.<br>
With no facet filter defined the query faceting is completely disabled, resulting in slightly better query performance.<br>
The values are sorted by the frequency of the appearance of the value within the indexed documents matching the query in descending order.

```rust
use seekstorm::search::QueryFacet;

let query_facets = vec![
    QueryFacet::String16 {
        field: "language".into(),
        prefix: "ger".into(),
        length: 10},
];
```

**Numerical range facets** require to **define the ranges** we want to distinguish and count and their labels at query time.

The ranges can be defined arbitrarily. Different bucket widths are allowed within the same range facet field.<br>
The **lower boundary** is defined explicitly (including the number itself).<br> 
The **upper boundary** is defined implicitly by the following boundary (excluding the number itself) or the maximum value allowed for the numeric type.

The ranges may be defined differently for each query. 
This allows to define facets ranges like last hour=current date - 1 hour, last day=current date - 24 hours, last week=current date - 168 hours, 
last month=current date - 5040 hours, where the meaning of **last hour** changes dynamically, 
when you look at the absolute value of the date of the indexed documents you want to include in the results. The number of distinct ranges per range facet field is limited to 65_536, excess ranges are ignored.

The **range_type** property of a QueryFacet allows to specify how to count the frequency of numerical facet field values: **within the specified range**, **within the range and all ranges above**, or **within the range and all ranges below**.

```rust
pub enum RangeType {
    CountWithinRange,
    CountAboveRange,
    CountBelowRange,
}
```

```rust
use seekstorm::search::{QueryFacet,RangeType};

let query_facets = vec![QueryFacet::U8 {
    field: "age".into(),
    range_type: RangeType::CountWithinRange,
    ranges: vec![
        ("0-20".into(), 0),
        ("20-40".into(), 20),
        ("40-60".into(), 40),
        ("60-80".into(), 60),
        ("80-100".into(), 80),
    ],
}];
```

**facet_filter**: Search results are filtered to documents matching specific values in the facet fields.

The filter parameter filters the returned results to those documents both matching the query AND matching for all (boolean AND) stated facet filter fields 
at least one (boolean OR) of the stated values.

If the **query is changed** and/or the **facet filter is changed** then both **search results and facet counts are changed**.

**String facet filter**
```rust
use seekstorm::search::FacetFilter;

let facet_filter = vec![FacetFilter::String16 {
    field: "age".into(),
    filter: vec!["62".into()],
}];
```

**Numerical range facet filter**
```rust
use seekstorm::search::FacetFilter;

let facet_filter = vec![FacetFilter::U8 {
    field: "age".into(),
    filter: 21..65,
}];
```


**result_sort**: Search results can be sorted by any numerical facet field, either in ascending or descending order.

If no sort field is specified, then the search results are sorted by rank in descending order per default.<br>
If there are multiple sort field specified, then the results are ordered by the first field, and then by the second field (tie-break).<br>
A special _score field (BM25x), reflecting how relevant the result is for a given search query (phrase match, match in title etc.) can be combined with any of the other sort fields as primary, secondary or n-th sort criterion.

**Result sort**
```rust
use seekstorm::search::{ResultSort,SortOrder,FacetValue};

let result_sort = vec![ResultSort {
    field: "age".into(),
    order: SortOrder::Ascending,
}];
```

**Query facets result object**: Query facets with counts for all distinct values are returned in **facets** property of the search result.

For all facet fields, it is counted how often each distinct value occurs **within all indexed documents matching the query**.
If the query is **not empty**, or if it is empty and **enable_empty_query** is true, then sorted/filtered **query facets** are returned.
If the query is **empty** and **enable_empty_query** is false, then no query is executed and sorted/filtered **index facets** are returned (like with get_index_string_facets).

Within each facet field, the values are sorted by their occurrence count **within all indexed documents matching the query** in descending order.
Numerical range & String value facet counting depend both on the query and on the applied range & string facet filtering.

**String facet result**
```json
[
  {
    "field": "fruit",
    "values": [
      [
        "Apple",
        41912
      ],
      [
        "Pear",
        41863
      ],
      [
        "Cherry",
        41524
      ],
      [
        "Orange",
        41516
      ],
      [
        "Plum",
        41502
      ]
    ]
  }
]
```

**Numerical range facet result**
```json
[
  {
    "field": "age",
    "values": [
      [
        "0-20",
        834014
      ],
      [
        "20-40",
        834091
      ],
      [
        "40-60",
        833311
      ],
      [
        "60-80",
        833288
      ],
      [
        "80-100",
        833363
      ]
    ]
  }
]
```





