use std::{fs::metadata, path::Path, sync::Arc};
use num_format::{Locale, ToFormattedString};
use tokio::sync::RwLock;
use crate::{index::{INDEX_FORMAT_VERSION_MAJOR, INDEX_FORMAT_VERSION_MINOR, Index, Info, META_FILENAME}, utils::dir_size, vector::Quantization, vector_similarity::VectorSimilarity};

/// Information collected independently of terminal rendering.
pub struct IndexInfo {
    /// Labels and values for index statistics.
    pub entries: Vec<Info>,
    /// Creation time, when the filesystem supports it.
    pub creation_time: Option<std::time::SystemTime>,
    /// Whether lexical indexing is enabled.
    pub lexical: bool,
    /// Whether vector indexing is enabled.
    pub vector: bool,
}

/// Collect index statistics without printing or depending on a terminal renderer.
pub async fn collect_index_info(
    index_arc: &Arc<RwLock<Index>>,
    time_label: &'static str,
    time_value: String,
) -> IndexInfo {
    let index_ref = index_arc.read().await;

    let non_affine_string = if index_ref.quantization == Quantization::ScalarQuantizationI8
        && index_ref.vector_similarity == VectorSimilarity::Euclidean
    {
        let non_affine = index_ref.shard_vec[0].read().await.max_vector_value == f32::MIN;
        if non_affine {
            " (non-affine)".to_string()
        } else {
            " (affine)".to_string()
        }
    } else {
        "".to_string()
    };

    let index_path = index_ref.index_path_string.clone();
    let level_count = index_ref.level_count().await;
    let file_date = metadata(Path::new(&index_path).join(META_FILENAME))
        .ok().and_then(|metadata| metadata.created().ok());

    for shard in index_ref.shard_vec.iter() {
        shard.read().await.index_file.sync_all().unwrap();
        shard.read().await.vector_file.sync_all().unwrap();
        shard.read().await.facets_file.sync_all().unwrap();
        shard.read().await.docstore_file.sync_all().unwrap();
    }
    let index_size = dir_size(Path::new(&index_ref.index_path_string)).unwrap_or(0);

    let dimensions_label = if index_ref.vector_dimensions != index_ref.vector_dimensions_original {
        " (".to_owned()
            + &index_ref.shard_vec[0]
                .read()
                .await
                .vector_dimensions
                .to_formatted_string(&Locale::en)
            + " padded)"
    } else {
        "".to_string()
    };

    let info_entries = vec![
        Info {
            entry: "Index",
            value: "".to_string(),
        },
        Info {
            entry: "index id, name",
            value: index_ref.meta.id.to_string() + " " + &index_ref.meta.name,
        },
        Info {
            entry: "index format version",
            value: index_ref.index_format_version_major.to_string()
                + "."
                + &index_ref.index_format_version_minor.to_string()
                + " "
                + &INDEX_FORMAT_VERSION_MAJOR.to_string()
                + "."
                + &INDEX_FORMAT_VERSION_MINOR.to_string(),
        },
        Info {
            entry: "indexed documents",
            value: index_ref
                .indexed_doc_count()
                .await
                .to_formatted_string(&Locale::en),
        },
        Info {
            entry: "deleted documents",
            value: index_ref.deleted_doc_count.to_formatted_string(&Locale::en),
        },
        Info {
            entry: "fields",
            value: index_ref.schema_map.len().to_string()
                + " ("
                + &index_ref.indexed_field_vec.len().to_string()
                + " indexed, "
                + &index_ref.stored_field_names.len().to_string()
                + " stored)",
        },
        Info {
            entry: "shards",
            value: index_ref.shard_count().await.to_string(),
        },
        Info {
            entry: "levels",
            value: level_count.to_string(),
        },
        Info {
            entry: "document compression",
            value: index_ref.meta.document_compression.to_string(),
        },
        Info {
            entry: "index size",
            value: index_size.to_formatted_string(&Locale::en) + " bytes",
        },
        Info {
            entry: "creation date",
            value: String::new(),
        },
        Info {
            entry: time_label,
            value: time_value,
        },
        Info {
            entry: "Lexical",
            value: index_ref.is_lexical_indexing.to_string(),
        },
        Info {
            entry: "segments",
            value: index_ref.segment_number1.to_string(),
        },
        Info {
            entry: "n-grams",
            value: format!("{:08b}", index_ref.meta.ngram_indexing),
        },
        Info {
            entry: "facets",
            value: index_ref.facets.len().to_string(),
        },
        Info {
            entry: "tokenizer",
            value: index_ref.meta.tokenizer.to_string(),
        },
        Info {
            entry: "stemmer",
            value: index_ref.meta.stemmer.to_string(),
        },
        Info {
            entry: "stop words",
            value: index_ref.shard_vec[0]
                .read()
                .await
                .stop_words
                .len()
                .to_string(),
        },
        Info {
            entry: "frequent words",
            value: index_ref.shard_vec[0]
                .read()
                .await
                .frequent_words
                .len()
                .to_string(),
        },
        Info {
            entry: "synonyms",
            value: index_ref.synonyms_map.len().to_string(),
        },
        Info {
            entry: "similarity",
            value: index_ref.meta.lexical_similarity.to_string(),
        },
        Info {
            entry: "Vector",
            value: index_ref.is_vector_indexing.to_string(),
        },
        Info {
            entry: "indexed vectors",
            value: index_ref
                .indexed_vector_count()
                .await
                .to_formatted_string(&Locale::en)
                + " ("
                + &(index_ref.indexed_vector_count().await / level_count.max(1))
                    .to_formatted_string(&Locale::en)
                + " per level)",
        },
        Info {
            entry: "indexed clusters",
            value: index_ref
                .indexed_cluster_count()
                .await
                .to_formatted_string(&Locale::en)
                + " ("
                + &(index_ref.indexed_cluster_count().await / level_count.max(1))
                    .to_formatted_string(&Locale::en)
                + " per level)",
        },
        Info {
            entry: "dimensions",
            value: index_ref
                .vector_dimensions_original
                .to_formatted_string(&Locale::en)
                + &dimensions_label,
        },
        Info {
            entry: "precision, quantization",
            value: index_ref.vector_precision.to_string()
                + ", "
                + &index_ref.quantization.to_string()
                + &non_affine_string,
        },
        Info {
            entry: "inference",
            value: index_ref.meta.inference.to_string(),
        },
        Info {
            entry: "clustering",
            value: index_ref.meta.clustering.to_string(),
        },
        Info {
            entry: "similarity",
            value: index_ref.vector_similarity.to_string(),
        },
    ];

    IndexInfo {
        entries: info_entries,
        creation_time: file_date,
        lexical: index_ref.is_lexical_indexing,
        vector: index_ref.is_vector_indexing,
    }
}
