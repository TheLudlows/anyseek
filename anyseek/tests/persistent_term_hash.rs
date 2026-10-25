use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Format-8 persistence contract. Cross-build test is driven twice with opposite
// AES flags; fixtures must be created by one executable and read by the other.
use anyseek::{
    index::{AccessType, Clustering, DocumentCompression,
        FieldLengthNormalization, FrequentwordType, IndexMetaObject,
        LexicalSimilarity, StopwordType, TextNormalization, TokenizerType,
        TERM_HASH_ALGORITHM},
    search::{QueryType, ResultType, SearchMode},
    vector::Inference,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};

fn config(normalization: TextNormalization) -> (IndexMetaObject, Vec<anyseek::index::SchemaField>) {
    let meta = IndexMetaObject {
        id:0, name:"stable-term-hash".into(), lexical_similarity:LexicalSimilarity::Bm25fProximity,
        tokenizer:TokenizerType::JiebaMixedV1, text_normalization:normalization,
        field_length_normalization:FieldLengthNormalization::PerField,
        stop_words:StopwordType::None, frequent_words:FrequentwordType::English,
        ngram_indexing:127, document_compression:DocumentCompression::Snappy,
        access_type:AccessType::Mmap, clustering:Clustering::None, inference:Inference::None,
    };
    let schema: Vec<anyseek::index::SchemaField>=serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"title","field_type":"Text","store":true,"index_lexical":true,"boost":2.0}
    ])).unwrap();
    (meta,schema)
}
async fn create(path:&Path, shards:usize, normalization:TextNormalization)->RecordPartition {
    let (meta,schema)=config(normalization);
    let index=Ok::<_, String>(records::create(path, meta, (&schema).to_vec()).await).unwrap();
    records::insert(&index, (0..80).map(|i|serde_json::from_value(json!({
        "title":if i%2==0 {"中国 北京 alpha"} else {"中國 東京 beta"},
        "body":format!("{} {} {}",if i%2==0 {"二战是什么时候开始的 中国 北京 the of and alpha beta"} else {"中國歷史 東京 the of and alpha gamma"},i,"longterm".repeat(100))
    })).unwrap()).collect()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        while index.statistics().await.unwrap().physical_versions!=80 {tokio::task::yield_now().await;}
    }).await.unwrap();
    index
}
async fn snapshot(index:&RecordPartition, realtime:bool)->Value {
    let mut rows=Vec::new();
    for (q,kind) in [("中国 北京",QueryType::Union),("alpha beta",QueryType::Intersection),
        ("the of and",QueryType::Phrase),("the of and",QueryType::Union),
        ("alpha -gamma",QueryType::Union),("二战是什么时候开始的",QueryType::Union),
        ("中國",QueryType::Union),("alpha",QueryType::Union)] {
        let r=index.query(RecordQuery {text:q.into(),vector:None,query_type:kind,mode:SearchMode::Lexical,offset:0,length:20,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
        assert!(!r.hits.is_empty(),"{q}");
        rows.push(json!({"query":q,"count":r.count_total,"hits":r.hits.iter().map(|x|(x.record.record_id.parse::<usize>().unwrap(),x.score.to_bits())).collect::<Vec<_>>()}));
    }
    json!(rows)
}
fn files(path:&Path)->BTreeMap<String,Vec<u8>> {
    fn visit(base:&Path,p:&Path,out:&mut BTreeMap<String,Vec<u8>>) {
        for e in std::fs::read_dir(p).unwrap() {let p=e.unwrap().path();if p.is_dir(){visit(base,&p,out)}else{out.insert(p.strip_prefix(base).unwrap().to_str().unwrap().into(),std::fs::read(p).unwrap());}}
    }
    let mut out=BTreeMap::new();visit(path,path,&mut out);out
}

#[tokio::test]
async fn format_gate_and_roundtrip() {
    assert_eq!(TERM_HASH_ALGORITHM,"xxh64-utf8-v1");
    let path=std::env::temp_dir().join(format!("anyseek-format8-{}",std::process::id()));assert!(!path.exists());
    let index=create(&path,3,TextNormalization::OpenCcT2s1_1_9).await;
    let _=snapshot(&index,true).await;
    index.compact_journal().await.unwrap();
    let expected=snapshot(&index,false).await;drop(index);
    let reopened=RecordPartition::open(&path, records::JOURNAL_LIMIT).await.unwrap();assert_eq!(snapshot(&reopened,false).await,expected);drop(reopened);
    std::fs::remove_dir_all(&path).unwrap();
    // Legacy raw index roots must not be interpreted as Record authority.
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("index.bin"), [8u8,0,0,0]).unwrap();
    let before=files(&path);
    assert!(RecordPartition::open(&path,records::JOURNAL_LIMIT).await.is_err());
    assert_eq!(files(&path),before);
    std::fs::remove_dir_all(path).unwrap();

}

#[tokio::test]
#[ignore = "run under opposite AES flags with TERM_HASH_FIXTURE and TERM_HASH_MODE"]
async fn cross_build_same_index() {
    let root=std::path::PathBuf::from(std::env::var_os("TERM_HASH_FIXTURE").expect("fixture path"));
    let mode=std::env::var("TERM_HASH_MODE").unwrap();assert!(mode=="create"||mode=="read");
    std::fs::create_dir_all(&root).unwrap();
    for shards in [1,3] {
        for (tag,normalization) in [("none",TextNormalization::None),("t2s",TextNormalization::OpenCcT2s1_1_9)] {
            let name=format!("{tag}-{shards}");let path=root.join(&name);let reference=root.join(format!("{name}.json"));
            if mode=="create" {
                assert!(!path.exists());let index=create(&path,shards,normalization).await;index.compact_journal().await.unwrap();
                let expected=snapshot(&index,false).await;drop(index);
                std::fs::write(reference,serde_json::to_vec(&expected).unwrap()).unwrap();
            } else {
                let before=files(&path);let index=RecordPartition::open(&path, records::JOURNAL_LIMIT).await.unwrap();
                let expected:Value=serde_json::from_slice(&std::fs::read(reference).unwrap()).unwrap();
                assert_eq!(snapshot(&index,false).await,expected,"{name}");drop(index);
                let _=before; // Record open may rebuild its derived local cache.
            }
        }
    }
}
