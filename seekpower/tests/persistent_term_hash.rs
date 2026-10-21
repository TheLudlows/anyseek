//! Format-8 persistence contract. Cross-build test is driven twice with opposite
//! AES flags; fixtures must be created by one executable and read by the other.
use seekpower::{
    commit::Commit,
    index::{create_index, open_index, AccessType, Close, Clustering, DocumentCompression,
        FieldLengthNormalization, FrequentwordType, IndexArc, IndexDocuments, IndexMetaObject,
        LexicalSimilarity, StopwordType, TextNormalization, TokenizerType,
        INDEX_FORMAT_VERSION_MAJOR, INDEX_FORMAT_VERSION_MINOR, TERM_HASH_ALGORITHM},
    search::{QueryType, ResultType, Search, SearchMode},
    vector::Inference,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};

fn config(normalization: TextNormalization) -> (IndexMetaObject, Vec<seekpower::index::SchemaField>) {
    let meta = IndexMetaObject {
        id:0, name:"stable-term-hash".into(), lexical_similarity:LexicalSimilarity::Bm25fProximity,
        tokenizer:TokenizerType::UnicodeAlphanumericZH, text_normalization:normalization,
        field_length_normalization:FieldLengthNormalization::PerField,
        stop_words:StopwordType::None, frequent_words:FrequentwordType::English,
        ngram_indexing:127, document_compression:DocumentCompression::Snappy,
        access_type:AccessType::Mmap, clustering:Clustering::None, inference:Inference::None,
    };
    let schema=serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"title","field_type":"Text","store":true,"index_lexical":true,"boost":2.0}
    ])).unwrap();
    (meta,schema)
}
async fn create(path:&Path, shards:usize, normalization:TextNormalization)->IndexArc {
    let (meta,schema)=config(normalization);
    let index=create_index(path,meta,&schema,11,true,Some(shards)).await.unwrap();
    index.index_documents((0..80).map(|i|serde_json::from_value(json!({
        "title":if i%2==0 {"中国 北京 alpha"} else {"中國 東京 beta"},
        "body":format!("{} {} {}",if i%2==0 {"二战是什么时候开始的 中国 北京 the of and alpha beta"} else {"中國歷史 東京 the of and alpha gamma"},i,"longterm".repeat(100))
    })).unwrap()).collect()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        while index.read().await.indexed_doc_count().await!=80 {tokio::task::yield_now().await;}
    }).await.unwrap();
    index
}
async fn snapshot(index:&IndexArc, realtime:bool)->Value {
    let mut rows=Vec::new();
    for (q,kind) in [("中国 北京",QueryType::Union),("alpha beta",QueryType::Intersection),
        ("the of and",QueryType::Phrase),("the of and",QueryType::Union),
        ("alpha -gamma",QueryType::Union),("二战是什么时候开始的",QueryType::Union),
        ("中國",QueryType::Union),("alpha",QueryType::Union)] {
        let r=index.search(q.into(),None,kind,SearchMode::Lexical,false,0,20,ResultType::TopkCount,realtime,vec![],vec![],vec![]).await;
        assert!(r.error.is_none(),"{q}: {:?}",r.error);
        assert!(!r.results.is_empty(),"{q}");
        rows.push(json!({"query":q,"count":r.result_count_total,"hits":r.results.iter().map(|x|(x.doc_id,x.score.to_bits())).collect::<Vec<_>>()}));
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
    assert_eq!((INDEX_FORMAT_VERSION_MAJOR,INDEX_FORMAT_VERSION_MINOR),(8,0));
    assert_eq!(TERM_HASH_ALGORITHM,"xxh64-utf8-v1");
    let path=std::env::temp_dir().join(format!("seekpower-format8-{}",std::process::id()));assert!(!path.exists());
    let index=create(&path,3,TextNormalization::OpenCcT2s1_1_9).await;
    let _=snapshot(&index,true).await;
    index.commit().await.unwrap();
    let expected=snapshot(&index,false).await;index.close().await.unwrap();drop(index);
    let reopened=open_index(&path).await.unwrap();assert_eq!(snapshot(&reopened,false).await,expected);reopened.close().await.unwrap();drop(reopened);
    let pristine=files(&path);
    // Root and every shard must reject legacy, future, minor mismatch, empty,
    // truncated and missing headers before any other file is changed/created.
    for relative in ["index.bin","shards/0/index.bin","shards/2/index.bin"] {
        let target=path.join(relative);let original=std::fs::read(&target).unwrap();
        for header in [Some(vec![7,0,0,0]),Some(vec![9,0,0,0]),Some(vec![8,0,1,0]),Some(vec![]),Some(vec![8,0]),None] {
            if let Some(header)=header {let mut bytes=header.clone();if header.len()==4{bytes.extend_from_slice(&original[4..]);}std::fs::write(&target,bytes).unwrap();}
            else {std::fs::remove_file(&target).unwrap();}
            let before=files(&path);
            let error=match open_index(&path).await {Err(e)=>e,Ok(_)=>panic!("accepted bad format")};
            assert!(error.contains("format header")||error.contains("unsupported index format"),"{error}");
            assert_eq!(files(&path),before,"open modified rejected index");
            let (meta,schema)=config(TextNormalization::OpenCcT2s1_1_9);
            assert!(create_index(&path,meta,&schema,11,true,Some(3)).await.is_err());
            assert_eq!(files(&path),before,"create modified rejected index");
            std::fs::write(&target,&original).unwrap();
        }
    }
    assert_eq!(files(&path),pristine);std::fs::remove_dir_all(path).unwrap();
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
                assert!(!path.exists());let index=create(&path,shards,normalization).await;index.commit().await.unwrap();
                let expected=snapshot(&index,false).await;index.close().await.unwrap();drop(index);
                std::fs::write(reference,serde_json::to_vec(&expected).unwrap()).unwrap();
            } else {
                let before=files(&path);let index=open_index(&path).await.unwrap();
                let expected:Value=serde_json::from_slice(&std::fs::read(reference).unwrap()).unwrap();
                assert_eq!(snapshot(&index,false).await,expected,"{name}");index.close().await.unwrap();drop(index);
                assert_eq!(files(&path),before,"reader changed committed index");
            }
        }
    }
}
