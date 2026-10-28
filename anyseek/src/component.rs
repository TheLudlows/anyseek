//! Private vector and metadata component. No lexical postings or original payloads.
//! The enclosing Record certificate authenticates these files; this descriptor is
//! never a business head and is only opened after the package closure is verified.
use crate::{index::*, vector::{Inference, Precision, Quantization, VectorHeader},
    vector_similarity::{TurboQuant, VectorSimilarity}, clustering::ParentMedoid,
    storage::build_budget::File, utils::*};
use ahash::{AHashMap, AHashSet};
use indexmap::IndexMap;
use memmap2::{Mmap, MmapMut, MmapOptions};
use serde::{Serialize, Deserialize};
use std::{collections::HashMap, fs, io::{Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}, sync::Arc};
use tokio::sync::RwLock;

const FORMAT: u32 = 1;
const DESCRIPTOR: &str = "component.json";
const VECTOR: &str = "vectors.bin";
const FACETS: &str = "metadata.bin";
pub(crate) type IndexArc = Arc<RwLock<Index>>;
pub(crate) type ShardArc = Arc<RwLock<Shard>>;

#[derive(Clone)]
pub(crate) struct Index {
    pub index_path_string: String,
    pub record_lexical: Vec<Arc<crate::record::lexical::runtime::Runtime>>,
    pub schema_map: HashMap<String, SchemaField>,
    pub stored_field_names: Vec<String>,
    pub meta: IndexMetaObject,
    pub shard_number: usize,
    pub shard_vec: Vec<ShardArc>,
    pub quantization: Quantization,
    pub vector_similarity: VectorSimilarity,
    pub is_simd: bool,
    pub is_vector_indexing: bool,
    pub turbo_quant: TurboQuant,
}

pub(crate) struct Shard {
    pub failed: bool,
    pub indexed_doc_count: usize,
    pub indexed_vector_count: usize,
    pub indexed_cluster_count: usize,
    pub committed_doc_count: usize,
    pub uncommitted: bool,
    pub schema_map: HashMap<String, SchemaField>,
    pub indexed_schema_vec: Vec<SchemaField>,
    #[cfg(test)]
    pub stored_field_names: Vec<String>,
    pub meta: IndexMetaObject,
    pub is_last_level_incomplete: bool,
    pub last_level_vector_file_start_pos: u64,
    pub vector_levels: usize,
    pub delete_hashset: AHashSet<usize>,
    pub active: Option<crate::active::ActiveStatistics>,
    pub build_reservation: Option<crate::storage::build_budget::MemoryLease>,
    pub build_input_covered: bool,
    path: PathBuf,
    pub facets: Vec<FacetField>,
    pub facets_map: HashMap<String, usize>,
    pub facets_size_sum: usize,
    facets_file: File,
    pub facets_file_mmap: MmapMut,
    pub string_set_to_single_term_id_vec: Vec<AHashMap<String, AHashSet<u32>>>,
    pub is_simd: bool,
    pub is_vector_indexing: bool,
    pub vector_file: File,
    pub vector_file_mmap: Mmap,
    pub block_vector_buffer: Vec<ParentMedoid>,
    pub vector_dimensions: usize,
    pub vector_dimensions_original: usize,
    pub vector_precision: Precision,
    pub quantization: Quantization,
    pub vector_similarity: VectorSimilarity,
    pub min_vector_value: f32,
    pub max_vector_value: f32,
    pub turbo_quant: TurboQuant,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    format: u32,
    meta: IndexMetaObject,
    schema: Vec<SchemaField>,
    documents: usize,
    vector_levels: usize,
    last_vector_level: u64,
    vectors: usize,
    clusters: usize,
    facets: Vec<FacetField>,
    min_vector_value: f32,
    max_vector_value: f32,
}

fn open_file(path: &Path, new: bool) -> Result<File,String> {
    File::options().read(true).write(true).create_new(new).open(path).map_err(|e|e.to_string())
}
fn vector_settings(meta: &IndexMetaObject) -> (usize, Precision, Quantization, VectorSimilarity) {
    match meta.inference {
        Inference::External { dimensions, precision, quantization, similarity } => (dimensions, precision, quantization, similarity),
        Inference::None => (0, Precision::None, Quantization::None, VectorSimilarity::Cosine),
    }
}
fn field_width(field: &FieldType) -> usize {
    match field {
        FieldType::U8|FieldType::I8 => 1,
        FieldType::U16|FieldType::I16|FieldType::String16|FieldType::StringSet16 => 2,
        FieldType::U32|FieldType::I32|FieldType::F32|FieldType::String32|FieldType::StringSet32 => 4,
        FieldType::U64|FieldType::I64|FieldType::F64|FieldType::Timestamp => 8,
        _ => 1,
    }
}
fn assemble(path: &Path, meta: IndexMetaObject, schema: &[SchemaField], saved: Option<Descriptor>) -> Result<IndexArc,String> {
    crate::vector::validate_vector_schema(&meta.inference,schema)?;
    if schema.iter().any(|f| f.index_lexical || f.store) { return Err("vector/metadata component cannot own lexical fields or payloads".into()); }
    let (original,precision,quantization,similarity)=vector_settings(&meta);
    let dimensions=if quantization==Quantization::TurboQuantI8 && precision==Precision::F32 {TurboQuant::next_power_of_two(original)}else{original};
    let turbo=TurboQuant::new(if quantization==Quantization::TurboQuantI8 {original}else{0},1234);
    let quantized=quantization!=Quantization::None || precision==Precision::I8;
    let lane=if *IS_AVX2 {if quantized {32}else{8}}else{if quantized {16}else{4}};
    let simd=*IS_SIMD && dimensions>0 && dimensions.is_multiple_of(lane);
    let mut schema_map=HashMap::new(); let mut indexed=Vec::new(); let mut facets=Vec::new(); let mut facets_map=HashMap::new(); let mut width=0;
    for (id,field) in schema.iter().enumerate() {
        let mut field=field.clone();field.field_id=id;field.indexed_field_id=indexed.len();
        if field.index_vector {indexed.push(field.clone());}
        if field.facet {
            facets_map.insert(field.field.clone(),facets.len());
            facets.push(FacetField {name:field.field.clone(),offset:width,field_type:field.field_type.clone(),values:IndexMap::new()});
            width+=field_width(&field.field_type);
        }
        schema_map.insert(field.field.clone(),field);
    }
    let new=saved.is_none();
    let mut vector_file=open_file(&path.join(VECTOR),new)?;
    let facets_file=open_file(&path.join(FACETS),new)?;
    if new { vector_file.write_all(&[0]).map_err(|e|e.to_string())?;vector_file.seek(SeekFrom::Start(0)).map_err(|e|e.to_string())?;
        if width>0 {facets_file.set_len((width*ROARING_BLOCK_SIZE) as u64).map_err(|e|e.to_string())?;}
    }
    let vector_map=unsafe{Mmap::map(&vector_file)}.map_err(|e|e.to_string())?;
    let facet_map=if width==0 {MmapOptions::new().len(1).map_anon()}else{unsafe{MmapMut::map_mut(&facets_file)}}.map_err(|e|e.to_string())?;
    let mut shard=Shard {failed:false,indexed_doc_count:0,indexed_vector_count:0,indexed_cluster_count:0,committed_doc_count:0,uncommitted:false,
        schema_map:schema_map.clone(),indexed_schema_vec:indexed,#[cfg(test)] stored_field_names:Vec::new(),meta:meta.clone(),is_last_level_incomplete:false,
        last_level_vector_file_start_pos:0,vector_levels:0,delete_hashset:AHashSet::new(),active:None,build_reservation:None,build_input_covered:false,
        path:path.to_owned(),facets,facets_map,facets_size_sum:width,facets_file,facets_file_mmap:facet_map,string_set_to_single_term_id_vec:Vec::new(),
        is_simd:simd,is_vector_indexing:original>0,vector_file,vector_file_mmap:vector_map,block_vector_buffer:Vec::new(),vector_dimensions:dimensions,
        vector_dimensions_original:original,vector_precision:precision,quantization,vector_similarity:similarity,min_vector_value:0.0,max_vector_value:f32::MIN,turbo_quant:turbo.clone()};
    if let Some(saved)=saved {
        if saved.vector_levels!=saved.documents.div_ceil(ROARING_BLOCK_SIZE) || saved.facets.len()!=shard.facets.len() {return Err("component cardinality mismatch".into());}
        for (actual,expected) in saved.facets.iter().zip(&shard.facets) {
            if actual.name!=expected.name {return Err("component metadata schema mismatch".into());}
        }
        if width.checked_mul(saved.documents).is_none_or(|n| n>shard.facets_file_mmap.len()) {return Err("component metadata truncated".into());}
        shard.indexed_doc_count=saved.documents;shard.committed_doc_count=saved.documents;
        shard.vector_levels=saved.vector_levels;shard.last_level_vector_file_start_pos=saved.last_vector_level;
        shard.indexed_vector_count=saved.vectors;shard.indexed_cluster_count=saved.clusters;
        shard.is_last_level_incomplete=!saved.documents.is_multiple_of(ROARING_BLOCK_SIZE);
        for (actual, expected) in saved.facets.iter().zip(&mut shard.facets) { expected.values=actual.values.clone(); }shard.min_vector_value=saved.min_vector_value;shard.max_vector_value=saved.max_vector_value;
        shard.validate_vectors()?;
    }
    shard.string_set_to_single_term_id();
    Ok(Arc::new(RwLock::new(Index {index_path_string:path.to_string_lossy().into_owned(),record_lexical:Vec::new(),schema_map,stored_field_names:Vec::new(),meta,shard_number:1,
        shard_vec:vec![Arc::new(RwLock::new(shard))],quantization,
        vector_similarity:similarity,is_simd:simd,is_vector_indexing:original>0,turbo_quant:turbo})))
}

pub(crate) async fn create_index(path:&Path,meta:IndexMetaObject,schema:&Vec<SchemaField>,shards:Option<usize>)->Result<IndexArc,String>{
    if shards!=Some(1) {return Err("Record component requires one physical partition".into());}
    let permit = match crate::resources::Scope::current() {
        Some(scope) if !scope.has_file_io_loan() => Some(scope.journal_io().await.map_err(|e|e.to_string())?),
        _ => None,
    };
    let create = || create_component_files(path, meta, schema);
    match permit { Some(permit) => permit.run(create), None => create() }
}
// Serial local file construction only: no acquired Index/Shard locks, native
// writer, spawned task, or remote I/O can be reached while this loan is held.
fn create_component_files(path:&Path,meta:IndexMetaObject,schema:&[SchemaField])->Result<IndexArc,String>{
    fs::create_dir(path).map_err(|e|e.to_string())?;
    // Keep the independently authenticated schema file for package validation.
    let schema_file=File::create(path.join("schema.json")).map_err(|e|e.to_string())?;
    serde_json::to_writer(&schema_file,schema).map_err(|e|e.to_string())?;schema_file.sync_all().map_err(|e|e.to_string())?;
    assemble(path,meta,schema,None)
}
pub(crate) async fn open_index(path:&Path)->Result<IndexArc,String>{
    let io=crate::resources::Scope::current().map(|scope|scope.io())
        .transpose().map_err(|e|e.to_string())?;
    let file=fs::File::open(path.join(DESCRIPTOR)).map_err(|e|e.to_string())?;
    let length=file.metadata().map_err(|e|e.to_string())?.len();
    if length>64*1024*1024 {return Err("component descriptor budget exceeded".into());}
    let estimate=length.checked_mul(8).and_then(|n|n.checked_add(4096)).ok_or("component descriptor size overflow")?;
    let _memory=crate::storage::build_budget::reserve_memory(estimate).map_err(|e|e.to_string())?;
    let _buffer=crate::resources::Scope::current().map(|s|s.retained_io().bytes(estimate))
        .transpose().map_err(|e|e.to_string())?;
    let mut bytes=Vec::with_capacity(length as usize);
    file.take(length+1).read_to_end(&mut bytes).map_err(|e|e.to_string())?;
    if bytes.len() as u64 != length {return Err("component descriptor changed while opening".into());}
    drop(io);
    let saved:Descriptor=serde_json::from_slice(&bytes).map_err(|e|e.to_string())?;
    if saved.format!=FORMAT {return Err("unsupported vector/metadata component format".into());}
    assemble(path,saved.meta.clone(),&saved.schema.clone(),Some(saved))
}

impl Shard {
    pub(crate) fn is_deleted(&self, id: usize) -> bool {
        self.delete_hashset.contains(&id) || self.active.as_ref().is_some_and(|active| !active.is_live(id))
    }
    pub(crate) fn rank(&self,a:f32,a_id:usize,b:f32,b_id:usize)->std::cmp::Ordering {
        crate::active::rank_with_keys(self.active.as_ref().map(|s|s.keys.as_slice()),a,a_id,b,b_id)
    }
    fn validate_vectors(&self)->Result<(),String>{
        if !self.is_vector_indexing {return Ok(());}
        let bytes=&self.vector_file_mmap;
        let precision=if self.quantization!=Quantization::None {Precision::I8}else{self.vector_precision};
        let size=size_of::<VectorHeader>()+self.vector_dimensions*if precision==Precision::F32 {4}else{1};
        let mut offset=0usize;let mut vectors=0usize;let mut clusters=0usize;let mut last=0;
        let read=|offset:&mut usize| -> Result<usize,String> {
            let end=offset.checked_add(4).ok_or("component vector offset overflow")?;
            let n=u32::from_le_bytes(bytes.get(*offset..end).ok_or("truncated vector component")?.try_into().unwrap()) as usize;
            *offset=end;Ok(n)
        };
        for _ in 0..self.vector_levels {
            last=offset as u64;let count=read(&mut offset)?;
            if count>self.indexed_vector_count.saturating_add(1) {return Err("invalid vector cluster count".into());}
            clusters=clusters.checked_add(count).ok_or("vector count overflow")?;
            let mut rows=0usize;
            for _ in 0..count {rows=rows.checked_add(read(&mut offset)?).ok_or("vector count overflow")?;}
            vectors=vectors.checked_add(rows).ok_or("vector count overflow")?;
            offset=rows.checked_mul(size).and_then(|n|offset.checked_add(n)).filter(|n|*n<=bytes.len()).ok_or("truncated vector rows")?;
        }
        if vectors!=self.indexed_vector_count || clusters!=self.indexed_cluster_count || (self.vector_levels>0 && last!=self.last_level_vector_file_start_pos) {
            return Err("vector component counters mismatch".into());
        }
        Ok(())
    }
    pub(crate) fn string_set_to_single_term_id(&mut self) {
        self.string_set_to_single_term_id_vec=vec![AHashMap::new();self.facets.len()];
        for (i,facet) in self.facets.iter().enumerate() {
            if matches!(facet.field_type,FieldType::StringSet16|FieldType::StringSet32) {
                for (id,(_,terms)) in facet.values.iter().enumerate() {
                    for term in terms {self.string_set_to_single_term_id_vec[i].entry(term.clone()).or_default().insert(id as u32);}
                }
            }
        }
    }
    async fn commit_component(&mut self)->std::io::Result<()> {
        let permit = match crate::resources::Scope::current() {
            Some(scope) if !scope.has_file_io_loan() => Some(scope.journal_io().await.map_err(std::io::Error::other)?),
            _ => None,
        };
        let result = match permit {
            Some(permit) => permit.run_async(self.commit_component_admitted()).await,
            None => self.commit_component_admitted().await,
        };
        if result.is_err() { self.failed = true; }
        result
    }
    async fn commit_component_admitted(&mut self)->std::io::Result<()> {
        if self.uncommitted {
            if self.is_vector_indexing { self.commit_vector_shard().await?; }
            if !self.is_last_level_incomplete {self.vector_levels+=1;}
            self.committed_doc_count=self.indexed_doc_count;
            self.is_last_level_incomplete=!self.committed_doc_count.is_multiple_of(ROARING_BLOCK_SIZE);
            self.uncommitted=false;
        }
        if self.facets_size_sum>0 {self.facets_file_mmap.flush()?;self.facets_file.sync_all()?;}
        let mut schema= self.schema_map.values().cloned().collect::<Vec<_>>();
        schema.sort_by_key(|field|field.field_id);
        let descriptor=Descriptor {format:FORMAT,meta:self.meta.clone(),schema,documents:self.indexed_doc_count,vector_levels:self.vector_levels,
            last_vector_level:self.last_level_vector_file_start_pos,vectors:self.indexed_vector_count,clusters:self.indexed_cluster_count,
            facets:self.facets.clone(),min_vector_value:self.min_vector_value,max_vector_value:self.max_vector_value};
        let file=File::create(self.path.join(DESCRIPTOR))?;
        serde_json::to_writer(&file,&descriptor)?;file.sync_all()?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn readonly_metadata_for_test(&mut self) {
        self.facets_file = File::open(self.path.join(FACETS)).unwrap();
    }
    fn ensure_metadata_capacity(&mut self, documents: usize)->Result<(),String>{
        if self.facets_size_sum==0 {return Ok(());}
        let bytes=documents.div_ceil(ROARING_BLOCK_SIZE).checked_mul(ROARING_BLOCK_SIZE)
            .and_then(|n|n.checked_mul(self.facets_size_sum)).ok_or("metadata capacity overflow")?;
        if bytes>self.facets_file_mmap.len() {
            self.facets_file_mmap.flush().map_err(|e|e.to_string())?;
            self.facets_file_mmap=MmapOptions::new().len(1).map_anon().map_err(|e|e.to_string())?;
            self.facets_file.set_len(bytes as u64).map_err(|e|e.to_string())?;
            self.facets_file_mmap=unsafe{MmapMut::map_mut(&self.facets_file)}.map_err(|e|e.to_string())?;
        }
        Ok(())
    }
    pub(crate) async fn append(&mut self, document:Document)->Result<(),String>{
        let permit = match crate::resources::Scope::current() {
            Some(scope) if !scope.has_file_io_loan() => Some(scope.journal_io().await.map_err(|e|e.to_string())?),
            _ => None,
        };
        match permit {
            Some(permit) => permit.run_async(self.append_admitted(document)).await,
            None => self.append_admitted(document).await,
        }
    }
    async fn append_admitted(&mut self, document:Document)->Result<(),String>{
        let ordinal=self.indexed_doc_count;
        if ordinal>0 && ordinal.is_multiple_of(ROARING_BLOCK_SIZE) && self.uncommitted {self.commit_component().await.map_err(|e|e.to_string())?;}
        // The full input is consumed by this call, never stored in the component.
        // Keep its transient lease across vector/indexing awaits, and retain only
        // actual vector/facet projection plus identity/row metadata afterwards.
        let input=crate::record::build_budget::document_estimate(&document,&[]).map_err(|e|e.to_string())?;
        let _input=crate::storage::build_budget::reserve_memory(input).map_err(|e|e.to_string())?;
        let estimate=crate::record::build_budget::component_estimate(&document,
            self.indexed_schema_vec.iter().map(|f|f.field.as_str()).chain(self.facets.iter().map(|f|f.name.as_str())))
            .map_err(|e|e.to_string())?;
        if !self.build_input_covered {if let Some(memory)=&mut self.build_reservation {memory.grow(estimate).map_err(|e|e.to_string())?;}}
        self.ensure_metadata_capacity(ordinal+1)?;
        if self.is_vector_indexing {self.index_vector_shard(ordinal,&document).await;}
        self.write_metadata(ordinal,&document)?;
        self.indexed_doc_count+=1;self.uncommitted=true;
        if let Some(active)=&mut self.active {active.publish(ordinal);}
        Ok(())
    }
    fn write_metadata(&mut self, docid_local:usize, document:&Document)->Result<(),String>{
        if !self.facets.is_empty() {
            let facets_size_sum = self.facets_size_sum;
            for i in 0..self.facets.len() {
                let facet = &mut self.facets[i];
                if let Some(field_value) = document.get(&facet.name) {
                    let address = (facets_size_sum * docid_local) + facet.offset;

                    match facet.field_type {
                        FieldType::U8 => {
                            let value = field_value.as_u64().unwrap_or_default() as u8;

                            self.facets_file_mmap[address] = value
                        }
                        FieldType::U16 => {
                            let value = field_value.as_u64().unwrap_or_default() as u16;

                            write_u16(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::U32 => {
                            let value = field_value.as_u64().unwrap_or_default() as u32;

                            write_u32(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::U64 => {
                            let value = field_value.as_u64().unwrap_or_default();

                            write_u64(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::I8 => {
                            let value = field_value.as_i64().unwrap_or_default() as i8;

                            write_i8(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::I16 => {
                            let value = field_value.as_i64().unwrap_or_default() as i16;

                            write_i16(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::I32 => {
                            let value = field_value.as_i64().unwrap_or_default() as i32;

                            write_i32(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::I64 => {
                            let value = field_value.as_i64().unwrap_or_default();

                            write_i64(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::Timestamp => {
                            let value = field_value.as_i64().unwrap_or_default();

                            write_i64(value, &mut self.facets_file_mmap, address);
                        }
                        FieldType::F32 => {
                            let value = field_value.as_f64().unwrap_or_default() as f32;

                            write_f32(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::F64 => {
                            let value = field_value.as_f64().unwrap_or_default();

                            write_f64(value, &mut self.facets_file_mmap, address)
                        }
                        FieldType::String16 if facet.values.len() < u16::MAX as usize => {
                            let key = serde_json::from_value::<String>(field_value.clone())
                                .unwrap_or(field_value.to_string());

                            let key_string = key.clone();
                            let key = vec![key];

                            facet.values.entry(key_string.clone()).or_insert(key);

                            let facet_value_id =
                                facet.values.get_index_of(&key_string).unwrap() as u16;
                            write_u16(facet_value_id, &mut self.facets_file_mmap, address)
                        }

                        FieldType::StringSet16 if facet.values.len() < u16::MAX as usize => {
                            let mut key: Vec<String> =
                                serde_json::from_value(field_value.clone()).unwrap();
                            key.sort();

                            let key_string = key.join("_");
                            let is_new = !facet.values.contains_key(&key_string);
                            let terms = key.clone();
                            facet.values.entry(key_string.clone()).or_insert(key);

                            let facet_value_id =
                                facet.values.get_index_of(&key_string).unwrap() as u16;
                            if is_new {
                                for term in terms {
                                    self.string_set_to_single_term_id_vec[i]
                                        .entry(term)
                                        .or_default()
                                        .insert(facet_value_id as u32);
                                }
                            }
                            write_u16(facet_value_id, &mut self.facets_file_mmap, address)
                        }

                        FieldType::String32 if facet.values.len() < u32::MAX as usize => {
                            let key = serde_json::from_value::<String>(field_value.clone())
                                .unwrap_or(field_value.to_string());

                            let key_string = key.clone();
                            let key = vec![key];

                            facet.values.entry(key_string.clone()).or_insert(key);

                            let facet_value_id =
                                facet.values.get_index_of(&key_string).unwrap() as u32;
                            write_u32(facet_value_id, &mut self.facets_file_mmap, address)
                        }

                        FieldType::StringSet32 if facet.values.len() < u32::MAX as usize => {
                            let mut key: Vec<String> =
                                serde_json::from_value(field_value.clone()).unwrap();
                            key.sort();

                            let key_string = key.join("_");
                            let is_new = !facet.values.contains_key(&key_string);
                            let terms = key.clone();
                            facet.values.entry(key_string.clone()).or_insert(key);

                            let facet_value_id =
                                facet.values.get_index_of(&key_string).unwrap() as u32;
                            if is_new {
                                for term in terms {
                                    self.string_set_to_single_term_id_vec[i]
                                        .entry(term)
                                        .or_default()
                                        .insert(facet_value_id);
                                }
                            }
                            write_u32(facet_value_id, &mut self.facets_file_mmap, address)
                        }

                        _ => {}
                    };
                }
            }
        }

        Ok(())
    }
}

#[allow(async_fn_in_trait)]
pub(crate) trait IndexDocuments { async fn index_documents(&self,documents:Vec<Document>)->Result<(),String>; }
impl IndexDocuments for IndexArc {
    async fn index_documents(&self,documents:Vec<Document>)->Result<(),String>{
        let root=self.read().await;
        if root.shard_vec.len()!=1 {return Err("writes require the private tail component".into());}
        let mut shard=root.shard_vec[0].write().await;
        for document in documents {shard.append(document).await?;}
        Ok(())
    }
}
#[allow(async_fn_in_trait)]
pub(crate) trait Commit {async fn commit(&self)->std::io::Result<()>;}
impl Commit for IndexArc {
    async fn commit(&self)->std::io::Result<()>{
        let root=self.read().await;
        for shard in &root.shard_vec {shard.write().await.commit_component().await?;}
        Ok(())
    }
}

#[cfg(test)]
mod descriptor_tests {
    #[tokio::test]
    async fn oversized_descriptor_rejected_before_reading_or_parsing() {
        let root=tempfile::tempdir().unwrap();
        let path=root.path().join(super::DESCRIPTOR);
        let file=std::fs::File::create(&path).unwrap();
        file.set_len(64*1024*1024+1).unwrap();drop(file);
        let error=match super::open_index(root.path()).await {Ok(_)=>panic!("oversized descriptor accepted"),Err(error)=>error};
        assert_eq!(error,"component descriptor budget exceeded");
        assert_eq!(std::fs::metadata(path).unwrap().len(),64*1024*1024+1);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(),1);
    }
}
