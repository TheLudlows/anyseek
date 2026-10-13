use chrono::{DateTime, Utc};
use seekstorm::{index::IndexArc, info::collect_index_info};
use tabled::{Table, Tabled, settings::{Color, Modify, Remove, Style, Width, object::{Columns, Rows}, style::{BorderColor, HorizontalLine}}};

#[derive(Tabled)]
pub(crate) struct Info {
    pub entry: &'static str,
    pub value: String,
}

pub(crate) async fn display_index_info(index: &IndexArc, time_label: &'static str, time_value: String) {
    let snapshot = collect_index_info(index, time_label, time_value).await;
    let info_entries: Vec<Info> = snapshot.entries.into_iter().map(|entry| Info {
        entry: entry.entry,
        value: if entry.entry == "creation date" {
            snapshot.creation_time.map(|time| DateTime::<Utc>::from(time).format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "unavailable".into())
        } else { entry.value },
    }).collect();
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

