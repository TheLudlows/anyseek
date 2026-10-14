use chrono::{DateTime, Utc};
use num_format::{Locale, ToFormattedString};
use seekstorm::{
    index::{INDEX_FORMAT_VERSION_MAJOR, INDEX_FORMAT_VERSION_MINOR, IndexArc},
    info::{IndexInfo, collect_index_info},
};
use tabled::{
    Table, Tabled,
    settings::{
        Color, Modify, Remove, Style, Width,
        object::{Columns, Rows},
        style::{BorderColor, HorizontalLine},
    },
};

#[derive(Tabled)]
pub(crate) struct Info {
    pub entry: &'static str,
    pub value: String,
}

pub(crate) async fn display_index_info(
    index: &IndexArc,
    time_label: &'static str,
    time_value: String,
) {
    let snapshot = collect_index_info(index).await;
    let info_entries = format_index_info(&snapshot, time_label, time_value);
    let mut table = Table::new(info_entries);
    table.modify(Columns::first(), Width::increase(26));

    table.modify(Columns::last(), Width::truncate(49).suffix("..."));
    table.modify(Columns::last(), Width::increase(50));

    table.with(Remove::row(Rows::first()));

    if snapshot.lexical && snapshot.vector {
        table
            .with(Style::modern().remove_horizontal().horizontals([
                (1, HorizontalLine::inherit(Style::modern())),
                (12, HorizontalLine::inherit(Style::modern())),
                (13, HorizontalLine::inherit(Style::modern())),
                (22, HorizontalLine::inherit(Style::modern())),
                (23, HorizontalLine::inherit(Style::modern())),
                (30, HorizontalLine::inherit(Style::modern())),
            ]))
            .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.modify(
            Columns::first(),
            BorderColor::filled(Color::FG_BRIGHT_BLACK),
        );
        table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.with(Modify::new(Rows::one(0)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(12)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(22)).with(Color::FG_CYAN));
    } else if snapshot.lexical {
        table.with(Remove::row(Rows::new(23..30)));
        table
            .with(Style::modern().remove_horizontal().horizontals([
                (1, HorizontalLine::inherit(Style::modern())),
                (12, HorizontalLine::inherit(Style::modern())),
                (13, HorizontalLine::inherit(Style::modern())),
                (22, HorizontalLine::inherit(Style::modern())),
                (23, HorizontalLine::inherit(Style::modern())),
                (24, HorizontalLine::inherit(Style::modern())),
            ]))
            .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.modify(
            Columns::first(),
            BorderColor::filled(Color::FG_BRIGHT_BLACK),
        );
        table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.with(Modify::new(Rows::one(0)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(12)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(22)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(23)).with(Color::FG_CYAN));
    } else if snapshot.vector {
        table.with(Remove::row(Rows::new(13..22)));
        table
            .with(Style::modern().remove_horizontal().horizontals([
                (1, HorizontalLine::inherit(Style::modern())),
                (12, HorizontalLine::inherit(Style::modern())),
                (13, HorizontalLine::inherit(Style::modern())),
                (14, HorizontalLine::inherit(Style::modern())),
                (21, HorizontalLine::inherit(Style::modern())),
                (22, HorizontalLine::inherit(Style::modern())),
            ]))
            .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.modify(
            Columns::first(),
            BorderColor::filled(Color::FG_BRIGHT_BLACK),
        );
        table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.with(Modify::new(Rows::one(0)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(12)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(13)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(21)).with(Color::FG_CYAN));
    } else {
        table.with(Remove::row(Rows::new(23..30)));
        table.with(Remove::row(Rows::new(13..22)));

        table
            .with(Style::modern().remove_horizontal().horizontals([
                (1, HorizontalLine::inherit(Style::modern())),
                (12, HorizontalLine::inherit(Style::modern())),
                (13, HorizontalLine::inherit(Style::modern())),
                (14, HorizontalLine::inherit(Style::modern())),
                (15, HorizontalLine::inherit(Style::modern())),
                (16, HorizontalLine::inherit(Style::modern())),
            ]))
            .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.modify(
            Columns::first(),
            BorderColor::filled(Color::FG_BRIGHT_BLACK),
        );
        table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.with(Modify::new(Rows::one(0)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(12)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(13)).with(Color::FG_CYAN));
        table.with(Modify::new(Rows::one(14)).with(Color::FG_CYAN));
    }

    println!("{}", table);
}

/// Human-readable labels, units and number/date formatting belong to the service.
fn format_index_info(info: &IndexInfo, time_label: &'static str, time_value: String) -> Vec<Info> {
    let padded = if info.vector_dimensions != info.padded_dimensions {
        format!(
            " ({} padded)",
            info.padded_dimensions.to_formatted_string(&Locale::en)
        )
    } else {
        String::new()
    };
    let affine = match info.scalar_affine {
        Some(true) => " (affine)",
        Some(false) => " (non-affine)",
        None => "",
    };
    let entries = [
        ("Index", String::new()),
        (
            "index id, name",
            format!("{} {}", info.meta.id, info.meta.name),
        ),
        (
            "index format version",
            format!(
                "{}.{} {}.{}",
                info.format_version.0,
                info.format_version.1,
                INDEX_FORMAT_VERSION_MAJOR,
                INDEX_FORMAT_VERSION_MINOR
            ),
        ),
        (
            "indexed documents",
            info.indexed_documents.to_formatted_string(&Locale::en),
        ),
        (
            "deleted documents",
            info.deleted_documents.to_formatted_string(&Locale::en),
        ),
        (
            "fields",
            format!(
                "{} ({} indexed, {} stored)",
                info.fields, info.indexed_fields, info.stored_fields
            ),
        ),
        ("shards", info.shards.to_string()),
        ("levels", info.levels.to_string()),
        (
            "document compression",
            info.meta.document_compression.to_string(),
        ),
        (
            "index size",
            format!(
                "{} bytes",
                info.index_size_bytes.to_formatted_string(&Locale::en)
            ),
        ),
        (
            "creation date",
            info.creation_time
                .map(|time| {
                    DateTime::<Utc>::from(time)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string()
                })
                .unwrap_or_else(|| "unavailable".into()),
        ),
        (time_label, time_value),
        ("Lexical", info.lexical.to_string()),
        ("segments", info.segments.to_string()),
        ("n-grams", format!("{:08b}", info.meta.ngram_indexing)),
        ("facets", info.metadata_fields.to_string()),
        ("tokenizer", info.meta.tokenizer.to_string()),
        ("stemmer", info.meta.stemmer.to_string()),
        ("stop words", info.stop_words.to_string()),
        ("frequent words", info.frequent_words.to_string()),
        ("synonyms", info.synonyms.to_string()),
        ("similarity", info.meta.lexical_similarity.to_string()),
        ("Vector", info.vector.to_string()),
        (
            "indexed vectors",
            format!(
                "{} ({} per level)",
                info.indexed_vectors.to_formatted_string(&Locale::en),
                (info.indexed_vectors / info.levels.max(1)).to_formatted_string(&Locale::en)
            ),
        ),
        (
            "indexed clusters",
            format!(
                "{} ({} per level)",
                info.indexed_clusters.to_formatted_string(&Locale::en),
                (info.indexed_clusters / info.levels.max(1)).to_formatted_string(&Locale::en)
            ),
        ),
        (
            "dimensions",
            format!(
                "{}{}",
                info.vector_dimensions.to_formatted_string(&Locale::en),
                padded
            ),
        ),
        (
            "precision, quantization",
            format!("{}, {}{}", info.precision, info.quantization, affine),
        ),
        ("inference", info.meta.inference.to_string()),
        ("clustering", info.meta.clustering.to_string()),
        ("similarity", info.vector_similarity.to_string()),
    ];
    entries
        .into_iter()
        .map(|(entry, value)| Info { entry, value })
        .collect()
}
