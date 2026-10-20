use seekpower::{commit::Commit,index::{create_index,open_index,AccessType,Close,Clustering,DocumentCompression,FrequentwordType,IndexDocuments,DeleteDocument,IndexMetaObject,LexicalSimilarity,StopwordType,TextNormalization,TokenizerType,FieldLengthNormalization},vector::Inference};
use serde_json::json;
use std::collections::HashSet;

#[tokio::test]
async fn projected_document_matches_full_decode_across_storage_states() {
 for (compression,label) in [(DocumentCompression::None,"plain"),(DocumentCompression::Snappy,"snappy")] {
  for shards in [1,3] {
   let path=std::env::temp_dir().join(format!("projection-{label}-{shards}-{}",std::process::id()));assert!(!path.exists());
   let meta=IndexMetaObject{id:0,name:"projection".into(),lexical_similarity:LexicalSimilarity::Bm25f,tokenizer:TokenizerType::UnicodeAlphanumeric,text_normalization:TextNormalization::None,field_length_normalization:FieldLengthNormalization::Shared,stop_words:StopwordType::None,frequent_words:FrequentwordType::None,ngram_indexing:0,document_compression:compression.clone(),access_type:AccessType::Mmap,clustering:Clustering::None,inference:Inference::None};
   let schema=serde_json::from_value(json!([
    {"field":"docid","field_type":"Text","store":true,"index_lexical":false},
    {"field":"body","field_type":"Text","store":true,"index_lexical":true},
    {"field":"nested","field_type":"Json","store":true,"index_lexical":false},
    {"field":"hidden","field_type":"Text","store":false,"index_lexical":false}
   ])).unwrap();
   let mut index=create_index(&path,meta,&schema,11,true,Some(shards)).await.unwrap();
   index.index_documents((0..12).map(|i|serde_json::from_value(json!({"docid":format!("00{i}"),"body":"alpha 中文 ".repeat(1000),"nested":{"array":[null,true,i,-0.0, {"é":"🦀"}]},"hidden":"not stored"})).unwrap()).collect()).await.unwrap();
   tokio::time::timeout(std::time::Duration::from_secs(10),async {while index.read().await.indexed_doc_count().await!=12 {tokio::task::yield_now().await;}}).await.unwrap();
   for stage in 0..3 {
    if stage==1 {index.commit().await;}
    if stage==2 {index.close().await;drop(index);index=open_index(&path).await.unwrap();}
    let guard=index.read().await;
    for id in 0..12 {
     if stage==0 {assert!(guard.get_document(id,false,&HashSet::new()).await.is_err());}
     for requested in [vec![],vec!["docid"],vec!["nested","docid"],vec!["missing"],vec!["body","nested","docid"],vec!["hidden"]] {
      let fields:HashSet<String>=requested.into_iter().map(String::from).collect();
      let mut expected=guard.get_document(id,true,&HashSet::new()).await.unwrap();
      if !fields.is_empty() {for key in guard.stored_field_names.iter() {if !fields.contains(key){expected.shift_remove(key);}}}
      let actual=guard.get_document(id,true,&fields).await.unwrap();
      assert_eq!(serde_json::to_vec(&actual).unwrap(),serde_json::to_vec(&expected).unwrap());
     }
    }
    assert!(guard.get_document(9999,true,&HashSet::from(["docid".to_string()])).await.is_err());
   }
   index.delete_document(0).await;
   assert!(index.read().await.get_document(0,true,&HashSet::from(["docid".to_string()])).await.is_err());
   index.close().await;drop(index);std::fs::remove_dir_all(path).unwrap();
  }
 }
}
