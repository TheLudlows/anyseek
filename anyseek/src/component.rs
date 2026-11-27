//! Retrieval topology and its private vector/metadata component. The component
//! files contain no lexical postings or original payloads.
//! The enclosing Record certificate authenticates these files; this descriptor is
//! never a business head and is only opened after the package closure is verified.
use crate::{index::*, vector::{Inference, Precision, Quantization},
    vector_similarity::{TurboQuant, VectorSimilarity}, clustering::ParentMedoid,
    record::{RecordError, build_budget::admit_memory}, resources::{MemoryReservation, Scope},
    storage::build_budget::File, utils::*};
use ahash::{AHashMap, AHashSet};
use indexmap::IndexMap;
use serde::{Serialize, Deserialize};
use std::{collections::HashMap, fs, io::{Read, Seek, SeekFrom, Write}, ops::Range, path::{Path, PathBuf}, sync::Arc};
use tokio::sync::RwLock;

pub(crate) mod memory;

const FORMAT: u32 = 3;
const DESCRIPTOR: &str = "component.json";
const VECTOR: &str = "vectors.bin";
const FACETS: &str = "metadata.bin";
const FACET_BLOCK_BYTES: usize = 4096;
pub(crate) type VectorIndexArc = Arc<RwLock<VectorIndex>>;
pub(crate) type VectorSegmentArc = Arc<RwLock<VectorSegment>>;

/// Query-local metadata reads bound to one guarded component. An isolated
/// lookup stays narrow. A second lookup in the same block enables reuse.
pub(crate) struct FacetReader<'a> {
    pub(crate) segment: &'a VectorSegment,
    previous_block: Option<usize>,
    cached: Option<FacetBlock>,
}

struct FacetBlock {
    bytes: Vec<u8>,
    start: usize,
    valid: usize,
    _memory: MemoryReservation,
}

impl<'a> FacetReader<'a> {
    pub(crate) fn new(segment: &'a VectorSegment) -> Self {
        Self { segment, previous_block: None, cached: None }
    }

    pub(crate) fn read(&mut self, ordinal: usize, field: &FacetField) -> Result<[u8; 9], RecordError> {
        let segment = self.segment;
        segment.read_facet_with(ordinal, field, |range, dst| {
            let start = range.start / FACET_BLOCK_BYTES * FACET_BLOCK_BYTES;
            let end = range.end - start;
            if end > FACET_BLOCK_BYTES {
                return segment.facets_file.read_exact_at(range.start as u64, dst).map_err(read_error);
            }
            let cached = self.cached.as_ref()
                .is_some_and(|block| block.start == start && end <= block.valid);
            if !cached {
                if self.previous_block.replace(start) != Some(start) {
                    return segment.facets_file.read_exact_at(range.start as u64, dst).map_err(read_error);
                }
                let length = segment.metadata_length()?;
                let capacity = length.min(FACET_BLOCK_BYTES);
                if self.cached.is_none() {
                    let memory = admit_memory(capacity as u64, Scope::current().as_ref())?;
                    self.cached = Some(FacetBlock { bytes: vec![0; capacity], start, valid: 0, _memory: memory });
                }
                let block = self.cached.as_mut().expect("admitted metadata block");
                let count = (length - start).min(capacity);
                block.valid = 0;
                segment.facets_file.read_exact_at(start as u64, &mut block.bytes[..count]).map_err(read_error)?;
                block.start = start;
                block.valid = count;
            }
            let block = self.cached.as_ref().expect("loaded metadata block");
            dst.copy_from_slice(&block.bytes[range.start - start..end]);
            Ok(())
        })
    }
}

/// A segment's vector/metadata component and lexical runtime share topology and
/// lifetime. Cloning a view retains both components, including native file pins.
#[derive(Clone)]
pub(crate) struct RetrievalSegment {
    pub vector: VectorSegmentArc,
    pub admission: Arc<memory::Admission>,
    pub lexical: Arc<crate::record::lexical::runtime::Runtime>,
    pub visibility: Arc<crate::record::lexical::visibility::PublishedSegment>,
}

#[derive(Clone)]
pub(crate) struct VectorIndex {
    pub index_path_string: String,
    pub schema_map: HashMap<String, SchemaField>,
    pub stored_field_names: Vec<String>,
    pub meta: IndexMetaObject,
    pub segments: Vec<RetrievalSegment>,
    pub quantization: Quantization,
    pub vector_similarity: VectorSimilarity,
    pub is_simd: bool,
    pub is_vector_indexing: bool,
    pub turbo_quant: TurboQuant,
}

/// Record transfers these pre-WAL leases; ordinary component callers retain
/// the existing admission path. No allocation is charged twice during transfer.
pub(crate) struct AppendMemory {
    pub input:crate::storage::build_budget::MemoryLease,
    pub retained:crate::storage::build_budget::MemoryLease,
    pub pending:memory::Pending,
}

pub(crate) struct VectorSegment {
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
    pub admission: Arc<memory::Admission>,
    path: PathBuf,
    pub facets: Vec<FacetField>,
    pub facets_map: HashMap<String, usize>,
    pub facets_size_sum: usize,
    facets_file: File,
    pub string_set_to_single_term_id_vec: Vec<AHashMap<String, AHashSet<u32>>>,
    pub is_simd: bool,
    pub is_vector_indexing: bool,
    pub vector_file: File,
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

/// I/O admission pressure is retryable, while short reads and corruption are storage faults.
pub(crate) fn read_error(error: std::io::Error) -> crate::record::RecordError {
    if error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<crate::resources::Busy>())
        .is_some_and(crate::resources::Busy::is_recoverable)
    {
        crate::record::RecordError::Busy
    } else {
        crate::record::RecordError::Storage(error.to_string())
    }
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
pub(crate) fn field_width(field: &FieldType) -> usize {
    match field {
        FieldType::String | FieldType::StringSet => 5,
        FieldType::Int | FieldType::Float | FieldType::Timestamp => 9,
        _ => 1,
    }
}
fn assemble(path: &Path, meta: IndexMetaObject, schema: &[SchemaField], saved: Option<Descriptor>, admission:Arc<memory::Admission>) -> Result<(VectorIndex, VectorSegmentArc),String> {
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
    let mut shard=VectorSegment {failed:false,indexed_doc_count:0,indexed_vector_count:0,indexed_cluster_count:0,committed_doc_count:0,uncommitted:false,
        schema_map:schema_map.clone(),indexed_schema_vec:indexed,#[cfg(test)] stored_field_names:Vec::new(),meta:meta.clone(),is_last_level_incomplete:false,
        last_level_vector_file_start_pos:0,vector_levels:0,delete_hashset:AHashSet::new(),active:None,admission,
        path:path.to_owned(),facets,facets_map,facets_size_sum:width,facets_file,string_set_to_single_term_id_vec:Vec::new(),
        is_simd:simd,is_vector_indexing:original>0,vector_file,block_vector_buffer:Vec::new(),vector_dimensions:dimensions,
        vector_dimensions_original:original,vector_precision:precision,quantization,vector_similarity:similarity,min_vector_value:0.0,max_vector_value:f32::MIN,turbo_quant:turbo.clone()};
    if let Some(saved)=saved {
        if saved.vector_levels!=saved.documents.div_ceil(ROARING_BLOCK_SIZE) || saved.facets.len()!=shard.facets.len() {return Err("component cardinality mismatch".into());}
        for (actual,expected) in saved.facets.iter().zip(&shard.facets) {
            if actual.name!=expected.name {return Err("component metadata schema mismatch".into());}
        }
        let metadata_len = shard.facets_file.metadata().map_err(|e| e.to_string())?.len();
        if width.checked_mul(saved.documents).is_none_or(|n| n as u64 > metadata_len) {return Err("component metadata truncated".into());}
        shard.indexed_doc_count=saved.documents;shard.committed_doc_count=saved.documents;
        shard.vector_levels=saved.vector_levels;shard.last_level_vector_file_start_pos=saved.last_vector_level;
        shard.indexed_vector_count=saved.vectors;shard.indexed_cluster_count=saved.clusters;
        shard.is_last_level_incomplete=!saved.documents.is_multiple_of(ROARING_BLOCK_SIZE);
        for (actual, expected) in saved.facets.iter().zip(&mut shard.facets) { expected.values=actual.values.clone(); }shard.min_vector_value=saved.min_vector_value;shard.max_vector_value=saved.max_vector_value;
    }
    shard.string_set_to_single_term_id();
    Ok((VectorIndex {index_path_string:path.to_string_lossy().into_owned(),schema_map,stored_field_names:Vec::new(),meta,
        segments:Vec::new(),quantization,
        vector_similarity:similarity,is_simd:simd,is_vector_indexing:original>0,turbo_quant:turbo}, Arc::new(RwLock::new(shard))))
}

pub(crate) async fn create_index(path:&Path,meta:IndexMetaObject,schema:&[SchemaField],config:&crate::record::RecordConfig)->Result<VectorIndexArc,crate::record::RecordError>{
    let permit = match crate::resources::Scope::current() {
        Some(scope) if !scope.has_file_io_loan() => Some(scope.journal_io().await.map_err(|e|crate::record::RecordError::Storage(e.to_string()))?),
        _ => None,
    };
    let admission=memory::Admission::new(schema.iter().filter(|f|f.index_vector).map(|f|f.field.as_str())
        .chain(schema.iter().filter(|f|f.facet).map(|f|f.field.as_str()))).map_err(crate::record::memory_error)?;
    let create = || create_component_files(path, meta, schema, admission);
    let (mut index, vector) = match permit { Some(permit) => permit.run(create), None => create() }.map_err(crate::record::RecordError::Storage)?;
    let lexical = crate::record::lexical::runtime::Runtime::create_owned(&path.join("native"), config)
        .await?;
    let admission=vector.read().await.admission.clone();
    index.segments.push(RetrievalSegment { vector, lexical, admission, visibility: Default::default() });
    Ok(Arc::new(RwLock::new(index)))
}
// Serial local file construction only: no acquired VectorIndex/VectorSegment locks, native
// writer, spawned task, or remote I/O can be reached while this loan is held.
fn create_component_files(path:&Path,meta:IndexMetaObject,schema:&[SchemaField],admission:Arc<memory::Admission>)->Result<(VectorIndex, VectorSegmentArc),String>{
    fs::create_dir(path).map_err(|e|e.to_string())?;
    // Keep the independently authenticated schema file for package validation.
    let schema_file=File::create(path.join("schema.json")).map_err(|e|e.to_string())?;
    serde_json::to_writer(&schema_file,schema).map_err(|e|e.to_string())?;schema_file.sync_all().map_err(|e|e.to_string())?;
    assemble(path,meta,schema,None,admission)
}
pub(crate) async fn open_index(path:&Path,config:&crate::record::RecordConfig)->Result<VectorIndexArc,crate::record::RecordError>{
    let (mut index, vector) = open_component_files(path)?;
    vector.read().await.validate_vectors()?;
    let lexical = crate::record::lexical::runtime::Runtime::open_owned(&path.join("native"), config)
        .await?;
    let admission=vector.read().await.admission.clone();
    index.segments.push(RetrievalSegment { vector, lexical, admission, visibility: Default::default() });
    Ok(Arc::new(RwLock::new(index)))
}

fn open_component_files(path:&Path)->Result<(VectorIndex, VectorSegmentArc),crate::record::RecordError>{
    use crate::record::RecordError;
    let storage=|error:String|RecordError::Storage(error);
    let io=crate::resources::Scope::current().map(|scope|scope.io())
        .transpose().map_err(|e|storage(e.to_string()))?;
    let file=fs::File::open(path.join(DESCRIPTOR)).map_err(|e|storage(e.to_string()))?;
    let length=file.metadata().map_err(|e|storage(e.to_string()))?.len();
    if length>64*1024*1024 {return Err(storage("component descriptor budget exceeded".into()));}
    let estimate=length.checked_mul(8).and_then(|n|n.checked_add(4096)).ok_or_else(||storage("component descriptor size overflow".into()))?;
    let _memory=crate::storage::build_budget::reserve_memory(estimate).map_err(crate::record::memory_error)?;
    let _buffer=crate::resources::Scope::current().map(|s|s.retained_io().bytes(estimate))
        .transpose().map_err(|e|storage(e.to_string()))?;
    let mut bytes=Vec::with_capacity(length as usize);
    file.take(length+1).read_to_end(&mut bytes).map_err(|e|storage(e.to_string()))?;
    if bytes.len() as u64 != length {return Err(storage("component descriptor changed while opening".into()));}
    drop(io);
    let saved:Descriptor=serde_json::from_slice(&bytes).map_err(|e|storage(e.to_string()))?;
    if saved.format!=FORMAT {return Err(storage("unsupported vector/metadata component format".into()));}
    let admission=memory::Admission::new(saved.schema.iter().filter(|f|f.index_vector).map(|f|f.field.as_str())
        .chain(saved.schema.iter().filter(|f|f.facet).map(|f|f.field.as_str()))).map_err(crate::record::memory_error)?;
    assemble(path,saved.meta.clone(),&saved.schema.clone(),Some(saved),admission).map_err(storage)
}

impl VectorSegment {
    pub(crate) fn is_deleted(&self, id: usize) -> bool {
        self.delete_hashset.contains(&id) || self.active.as_ref().is_some_and(|active| !active.is_live(id))
    }
    pub(crate) fn rank(&self,a:f32,a_id:usize,b:f32,b_id:usize)->std::cmp::Ordering {
        crate::active::rank_with_keys(self.active.as_ref().map(|s|s.keys.as_slice()),a,a_id,b,b_id)
    }
    pub(crate) fn string_set_to_single_term_id(&mut self) {
        self.string_set_to_single_term_id_vec=vec![AHashMap::new();self.facets.len()];
        for (i,facet) in self.facets.iter().enumerate() {
            if matches!(facet.field_type,FieldType::StringSet) {
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
        if result.is_err() { self.failed = true; self.admission.fail(); }
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
        if self.facets_size_sum>0 {self.facets_file.sync_all()?;}
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
    /// Read one fixed-width facet value and its presence byte without allocating.
    pub(crate) fn read_facet(
        &self,
        ordinal: usize,
        field: &FacetField,
    ) -> Result<[u8; 9], RecordError> {
        self.read_facet_with(ordinal, field, |range, dst| {
            self.facets_file.read_exact_at(range.start as u64, dst).map_err(read_error)
        })
    }

    fn metadata_length(&self) -> Result<usize, RecordError> {
        self.indexed_doc_count.checked_mul(self.facets_size_sum)
            .ok_or_else(|| RecordError::Storage("metadata length overflow".into()))
    }

    fn read_facet_with(
        &self,
        ordinal: usize,
        field: &FacetField,
        read: impl FnOnce(Range<usize>, &mut [u8]) -> Result<(), RecordError>,
    ) -> Result<[u8; 9], RecordError> {
        let width = field_width(&field.field_type);
        let offset = ordinal
            .checked_mul(self.facets_size_sum)
            .and_then(|row| row.checked_add(field.offset))
            .ok_or_else(|| RecordError::Storage("metadata offset overflow".into()))?;
        let length = self.metadata_length()?;
        let end = offset.checked_add(width).filter(|end| *end <= length)
            .ok_or_else(|| RecordError::Storage("metadata field outside component rows".into()))?;
        let mut bytes = [0; 9];
        read(offset..end, &mut bytes[..width])?;
        if bytes[width - 1] > 1 {
            return Err(RecordError::Storage(
                "invalid metadata presence byte".into(),
            ));
        }
        Ok(bytes)
    }

    pub(crate) async fn append(&mut self, document:Document)->Result<(),String>{
        self.append_with_memory(document,None).await
    }
    pub(crate) async fn append_with_memory(&mut self, document:Document, memory:Option<AppendMemory>)->Result<(),String>{
        let permit = match crate::resources::Scope::current() {
            Some(scope) if !scope.has_file_io_loan() => Some(scope.journal_io().await.map_err(|e|e.to_string())?),
            _ => None,
        };
        match permit {
            Some(permit) => permit.run_async(self.append_admitted(document,memory)).await,
            None => self.append_admitted(document,memory).await,
        }
    }
    async fn append_admitted(&mut self, document:Document, memory:Option<AppendMemory>)->Result<(),String>{
        crate::index::validate_field_values(self.schema_map.values(), &document)?;
        // Rebuild/replay can enter below the public write validator.
        if self.vector_similarity == VectorSimilarity::Euclidean && self.vector_precision == Precision::F32 {
            crate::vector::validate_vectors_in_schema(&self.meta.inference, self.schema_map.values(), &document)?;
        }
        let ordinal=self.indexed_doc_count;
        if ordinal>0 && ordinal.is_multiple_of(ROARING_BLOCK_SIZE) && self.uncommitted {self.commit_component().await.map_err(|e|e.to_string())?;}
        // The full input is consumed by this call, never stored in the component.
        // Keep its transient lease across vector/indexing awaits, and retain only
        // actual vector/facet projection plus identity/row metadata afterwards.
        let input=crate::record::build_budget::document_estimate(&document,&[]).map_err(|e|e.to_string())?;
        let estimate=crate::record::build_budget::component_estimate(&document,
            self.indexed_schema_vec.iter().map(|f|f.field.as_str()).chain(self.facets.iter().map(|f|f.name.as_str())))
            .map_err(|e|e.to_string())?;
        let _input=match memory {
            Some(mut memory)=>{
                if memory.input.bytes()<input || memory.retained.bytes()<estimate {
                    return Err("component pre-WAL memory bound exceeded".into());
                }
                memory.pending.transfer(&self.admission,memory.retained).map_err(|e|e.to_string())?;
                memory.input
            },
            None=>{
                let input=crate::storage::build_budget::reserve_memory(input).map_err(|e|e.to_string())?;
                self.admission.grow(estimate,true).map_err(|e|e.to_string())?;
                input
            },
        };
        let mutation=self.admission.mutation();
        if self.is_vector_indexing {self.index_vector_segment(ordinal,&document).await;}
        self.write_metadata(ordinal,&document)?;
        self.indexed_doc_count+=1;self.uncommitted=true;
        if let Some(active)=&mut self.active {active.publish(ordinal);}
        mutation.complete();
        Ok(())
    }
    fn write_metadata(&mut self, docid_local: usize, document: &Document) -> Result<(), String> {
        if self.facets.is_empty() {
            return Ok(());
        }
        let start = self
            .facets_size_sum
            .checked_mul(docid_local)
            .ok_or("metadata offset overflow")?;
        let end = start
            .checked_add(self.facets_size_sum)
            .ok_or("metadata length overflow")?;
        if end as u64
            > self
                .facets_file
                .metadata()
                .map_err(|e| e.to_string())?
                .len()
        {
            let capacity = (docid_local + 1)
                .div_ceil(ROARING_BLOCK_SIZE)
                .checked_mul(ROARING_BLOCK_SIZE)
                .and_then(|n| n.checked_mul(self.facets_size_sum))
                .ok_or("metadata capacity overflow")?;
            self.facets_file
                .set_len(capacity as u64)
                .map_err(|e| e.to_string())?;
        }
        self.facets_file
            .seek(SeekFrom::Start(start as u64))
            .map_err(|e| e.to_string())?;
        for (i, facet) in self.facets.iter_mut().enumerate() {
            let mut bytes = [0; 9];
            let width = field_width(&facet.field_type);
            if let Some(field_value) = document.get(&facet.name).filter(|v| !v.is_null()) {
                bytes[width - 1] = 1;
                match facet.field_type {
                    FieldType::Int | FieldType::Timestamp => {
                        let value = field_value
                            .as_i64()
                            .ok_or("metadata integer exceeds signed 64-bit range")?;
                        write_i64(value, &mut bytes, 0);
                    }
                    FieldType::Float => {
                        let value = field_value
                            .as_f64()
                            .filter(|v| v.is_finite())
                            .ok_or("invalid metadata Float")?;

                        write_f64(value, &mut bytes, 0)
                    }

                    FieldType::String => {
                        let key = field_value
                            .as_str()
                            .ok_or("invalid metadata String")?
                            .to_owned();

                        let key_string = key.clone();
                        let key = vec![key];

                        if !facet.values.contains_key(&key_string)
                            && facet.values.len() >= u32::MAX as usize
                        {
                            return Err("metadata dictionary capacity exceeded".into());
                        }
                        facet.values.entry(key_string.clone()).or_insert(key);

                        let facet_value_id = facet.values.get_index_of(&key_string).unwrap() as u32;
                        write_u32(facet_value_id, &mut bytes, 0)
                    }

                    FieldType::StringSet => {
                        let mut key: Vec<String> = serde_json::from_value(field_value.clone())
                            .map_err(|e| e.to_string())?;
                        key.sort();
                        key.dedup();

                        let key_string = serde_json::to_string(&key).map_err(|e| e.to_string())?;
                        let is_new = !facet.values.contains_key(&key_string);
                        let terms = key.clone();
                        if !facet.values.contains_key(&key_string)
                            && facet.values.len() >= u32::MAX as usize
                        {
                            return Err("metadata dictionary capacity exceeded".into());
                        }
                        facet.values.entry(key_string.clone()).or_insert(key);

                        let facet_value_id = facet.values.get_index_of(&key_string).unwrap() as u32;
                        if is_new {
                            for term in terms {
                                self.string_set_to_single_term_id_vec[i]
                                    .entry(term)
                                    .or_default()
                                    .insert(facet_value_id);
                            }
                        }
                        write_u32(facet_value_id, &mut bytes, 0)
                    }

                    _ => {}
                };
            }
            self.facets_file
                .write_all(&bytes[..width])
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }


}

#[allow(async_fn_in_trait)]
pub(crate) trait IndexDocuments { async fn index_documents(&self,documents:Vec<Document>)->Result<(),String>; }
impl IndexDocuments for VectorIndexArc {
    async fn index_documents(&self,documents:Vec<Document>)->Result<(),String>{
        let root=self.read().await;
        if root.segments.len()!=1 {return Err("writes require the private tail component".into());}
        let mut shard=root.segments[0].vector.write().await;
        for document in documents {shard.append(document).await?;}
        Ok(())
    }
}
#[allow(async_fn_in_trait)]
pub(crate) trait Commit {async fn commit(&self)->std::io::Result<()>;}
impl Commit for VectorIndexArc {
    async fn commit(&self)->std::io::Result<()>{
        let root=self.read().await;
        for shard in &root.segments {shard.vector.write().await.commit_component().await?;}
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
        let error=match super::open_component_files(root.path()) {Ok(_)=>panic!("oversized descriptor accepted"),Err(error)=>error};
        assert!(matches!(error,crate::record::RecordError::Storage(ref message) if message=="component descriptor budget exceeded"));
        assert_eq!(std::fs::metadata(path).unwrap().len(),64*1024*1024+1);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(),1);
    }
}

impl Drop for VectorSegment {fn drop(&mut self){self.admission.close();}}
