use base64::{Engine, engine::general_purpose};
use colored::Colorize;
use crossbeam_channel::{Receiver, bounded, select};

use indexmap::IndexMap;

use num_cpus::get_physical;
use num_format::{Locale, ToFormattedString};

use crate::multi_tenancy::ApikeyObject;

use seekstorm::{
    index::{
        Close, IS_AVX2, IS_NEON, Info,
    },
    info::display_index_info,
    search::{QueryType, ResultType, Search, SearchMode},
    utils::dir_size,
    vector_similarity::AnnMode,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env::current_exe,
    fs,
    io::{self, IsTerminal},
    path::Path,
    sync::Arc,
    thread::available_parallelism,
    time::Instant,
};
use tabled::{
    Table,
    settings::{
        Color, Modify, Remove, Style, Width,
        object::{Columns, Rows},
        style::{BorderColor, HorizontalLine},
    },
};
use tokio::sync::RwLock;

use crate::{
    MASTER_KEY_SECRET, VERSION,
    api_endpoints::{
        create_apikey_api, delete_apikey_api, generate_openapi, open_all_apikeys,
    },
    http_server::{calculate_hash, http_server},
    multi_tenancy::get_apikey_hash,
};


fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(20);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

async fn commandline(sender: crossbeam_channel::Sender<String>) {
    #[allow(clippy::manual_flatten)]
    for line in io::stdin().lines() {
        if let Ok(line) = line {
            if sender.send(line.clone()).is_err() {
                return;
            }

            if line.to_lowercase() == "quit" {
                return;
            }
        } else {
            println!("stdin read error, try using -ti parameter in docker environment");
            break;
        }
    }
}

pub(crate) async fn initialize(params: HashMap<String, String>) {
    if params.contains_key("ingest_path") {
        eprintln!("ingest_path has been removed; prepare records externally and POST JSON to /api/v1/index/{{index_id}}/doc");
        std::process::exit(2);
    }
    let force_shard_number: Option<usize> = params
        .get("force_shard_number")
        .and_then(|cores| cores.parse::<usize>().ok());

    let mut index_path_str = "seekstorm_index";
    if params.contains_key("index_path") {
        index_path_str = params.get("index_path").unwrap();
    }
    let mut index_path = Path::new(&index_path_str);
    let mut absolute_path = current_exe().unwrap();
    if !index_path.is_absolute() {
        absolute_path.pop();
        absolute_path.push(index_path_str);
        index_path = &absolute_path;
    }

    if !index_path.exists() {
        match fs::create_dir_all(index_path) {
            Ok(_v) => {}
            Err(_e) => {
                println!("index_path could not be created: {}", index_path.display());
            }
        }

        println!(
            "index_path did not exists, new directory created: {}",
            index_path.display()
        );
    }

    let apikey_list_map: HashMap<u128, ApikeyObject> = HashMap::new();
    let apikey_list = Arc::new(RwLock::new(apikey_list_map));
    let apikey_list_clone = apikey_list.clone();

    let mut local_ip = "0.0.0.0".to_string();
    let mut local_port = 80;
    if params.contains_key("local_ip") {
        local_ip = params.get("local_ip").unwrap().to_string();
    }
    if params.contains_key("local_port") {
        local_port = params.get("local_port").unwrap().parse::<u16>().unwrap();
    }

    let index_path = Path::new(&index_path).to_path_buf();

    let mut hasher = Sha256::new();
    hasher.update(MASTER_KEY_SECRET.to_string());
    let master_apikey = hasher.finalize();
    let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

    let info_entries = vec![
        Info {
            entry: "SeekStorm server",
            value: VERSION.to_string(),
        },
        Info {
            entry: "index path",
            value: index_path.display().to_string(),
        },
        Info {
            entry: "SIMD / processor",
            value: if *IS_AVX2 {
                "AVX2".to_string()
            } else if *IS_NEON {
                "NEON".to_string()
            } else {
                "disabled".to_string()
            } + " / "
                + num_cpus::get().to_string().as_str()
                + " logical, "
                + get_physical().to_string().as_str()
                + " physical cores, "
                + available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or_else(|_| num_cpus::get())
                    .to_string()
                    .as_str(),
        },
        Info {
            entry: "web server (UI, REST API) ",
            value: format!(
                "http://localhost:{}  ({}:{})",
                local_port, local_ip, local_port
            ),
        },
        Info {
            entry: "master API key ⚠️",
            value: master_apikey_base64.clone(),
        },
        Info {
            entry: "Help",
            value: "Enter 'help' for console commands".to_string(),
        },
        Info {
            entry: "Shutdown server",
            value: "Enter 'quit' or press CTRL-C".to_string(),
        },
    ];

    let mut table = Table::new(info_entries);

    table
        .with(
            Style::modern()
                .remove_horizontal()
                .horizontals([(1, HorizontalLine::inherit(Style::modern()))]),
        )
        .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));
    table.modify(
        Columns::first(),
        BorderColor::filled(Color::FG_BRIGHT_BLACK),
    );
    table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

    table.modify(Columns::first(), Width::increase(25));
    table.modify(Columns::last(), Width::truncate(49).suffix("..."));
    table.modify(Columns::last(), Width::increase(50));

    table.with(Remove::row(Rows::first()));
    table.with(Modify::new(Rows::one(0)).with(Color::FG_BRIGHT_GREEN));
    table.with(Modify::new(Rows::one(5)).with(Color::FG_BRIGHT_RED));
    table.with(Modify::new(Rows::one(6)).with(Color::FG_YELLOW));
    table.with(Modify::new(Rows::one(7)).with(Color::FG_YELLOW));

    println!("{}", table);

    let mut apikey_list_mut = apikey_list.write().await;

    let start_time = Instant::now();
    open_all_apikeys(&index_path, &mut apikey_list_mut).await;
    let elapsed_time = start_time.elapsed().as_nanos();

    let tenants_count = apikey_list_mut.len();
    let mut index_count = 0;
    for key in apikey_list_mut.iter() {
        index_count += key.1.index_list.len();
    }

    let index_size = dir_size(Path::new(&index_path)).unwrap_or(0);

    let info_entries = vec![
        Info {
            entry: "Multitenancy",
            value: "".to_string(),
        },
        Info {
            entry: "Number of tenants",
            value: tenants_count.to_string(),
        },
        Info {
            entry: "Number of indices",
            value: index_count.to_string(),
        },
        Info {
            entry: "Overall disk usage",
            value: index_size.to_formatted_string(&Locale::en) + " bytes",
        },
    ];

    let mut table = Table::new(info_entries);

    table
        .with(
            Style::modern()
                .remove_horizontal()
                .horizontals([(1, HorizontalLine::inherit(Style::modern()))]),
        )
        .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));
    table.modify(
        Columns::first(),
        BorderColor::filled(Color::FG_BRIGHT_BLACK),
    );
    table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

    table.modify(Columns::first(), Width::increase(26));
    table.modify(Columns::last(), Width::truncate(49).suffix("..."));
    table.modify(Columns::last(), Width::increase(50));

    table.with(Remove::row(Rows::first()));
    table.with(Modify::new(Rows::one(0)).with(Color::FG_CYAN));

    println!("{}", table);

    let time_label: &'static str = "load time";
    let time_value = (elapsed_time / 1_000_000_000).to_string() + " s";
    for apikey in apikey_list_mut.iter() {
        let index_size =
            dir_size(Path::new(&index_path).join(apikey.1.id.to_string())).unwrap_or(0) as usize;

        let mut indexed_doc_count_sum = 0;
        for index in apikey.1.index_list.iter() {
            indexed_doc_count_sum += index.1.read().await.indexed_doc_count().await;
        }

        let info_entries = vec![
            Info {
                entry: "Tenant",
                value: "".to_string(),
            },
            Info {
                entry: "tenant id",
                value: apikey.1.id.to_string(),
            },
            Info {
                entry: "indices",
                value: apikey.1.index_list.len().to_string(),
            },
            Info {
                entry: "documents",
                value: indexed_doc_count_sum.to_formatted_string(&Locale::en),
            },
            Info {
                entry: "disk usage",
                value: index_size.to_formatted_string(&Locale::en) + " bytes",
            },
        ];

        let mut table = Table::new(info_entries);

        table
            .with(
                Style::modern()
                    .remove_horizontal()
                    .horizontals([(1, HorizontalLine::inherit(Style::modern()))]),
            )
            .with(BorderColor::filled(Color::FG_BRIGHT_BLACK));
        table.modify(
            Columns::first(),
            BorderColor::filled(Color::FG_BRIGHT_BLACK),
        );
        table.modify(Columns::last(), BorderColor::filled(Color::FG_BRIGHT_BLACK));

        table.modify(Columns::first(), Width::increase(26));
        table.modify(Columns::last(), Width::truncate(49).suffix("..."));
        table.modify(Columns::last(), Width::increase(50));

        table.with(Remove::row(Rows::first()));
        table.with(Modify::new(Rows::one(0)).with(Color::FG_CYAN));

        println!("{}", table);

        for index in apikey.1.index_list.iter() {
            display_index_info(index.1, time_label, time_value.clone()).await;
        }
    }

    drop(apikey_list_mut);

    let stdin = io::stdin();
    let terminal = stdin.is_terminal();
    let receiver_ctrl_c = ctrl_channel().unwrap();
    let (sender_commandline, receiver_commandline) = bounded(20);
    if terminal {
        tokio::spawn(async { commandline(sender_commandline).await });
    } else {
        println!(
            "Standard input is not a terminal. Console commands are disabled. Use docker parameter '-ti terminal/tti' to enable."
        );
    }

    let index_path_local = index_path.clone();

    tokio::spawn(async move {
        http_server(
            &index_path_local,
            apikey_list,
            &local_ip,
            &local_port,
            &force_shard_number,
        )
        .await
    });

    let demo_api_key = [0u8; 32];
    let demo_api_key_base64 = general_purpose::STANDARD.encode(demo_api_key);

    loop {
        if terminal {
            select! {

                recv(receiver_ctrl_c) -> _ => {
                    println!("Committing all indices ...");
                    let mut apikey_list_mut=apikey_list_clone.write().await;
                    for apikey in apikey_list_mut.iter_mut()
                    {
                        for index in apikey.1.index_list.iter_mut()
                        {
                            index.1.close().await;
                        }
                    }
                    drop(apikey_list_mut);

                    println!("Server stopped by Ctrl-C");
                    return;
                }


                recv(receiver_commandline) -> message => {

                    if let Ok(m) = message {
                    let parameter:Vec<&str>=m.split_whitespace().collect();
                    let command=if parameter.is_empty() {""} else {&parameter[0].to_lowercase()};

                    let mut dash:HashMap<String,String>=HashMap::new();
                    for (i,component) in parameter.iter().enumerate(){
                        if let Some(key_stripped) = component.strip_prefix("-") && i+1<parameter.len() {dash.insert(key_stripped.to_string(),parameter[i+1].to_string());}
                    }

                    match command
                    {
                        "search" =>
                        {
                            println!("search start");
                            let mut apikey_list_mut = apikey_list_clone.write().await;
                            let apikey_hash = calculate_hash(&demo_api_key) as u128;
                            if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                                let index_id=0;
                                if let Some(index_arc) = apikey_object.index_list.get_mut(&index_id) {


                                    let Some(raw_vector) = dash.get("v") else {
                                        eprintln!("search requires an external vector: search -v [1,0,0] (no spaces in JSON)");
                                        continue;
                                    };
                                    let value = match serde_json::from_str::<serde_json::Value>(raw_vector) {
                                        Ok(value) => value,
                                        Err(error) => { eprintln!("invalid vector JSON: {error}"); continue; }
                                    };
                                    let query_vector = match seekstorm::vector::parse_external_embedding(&value, &index_arc.read().await.meta.inference) {
                                        Ok(vector) => vector,
                                        Err(error) => { eprintln!("invalid vector: {error}"); continue; }
                                    };
                                    let query="";
                                    let len=10;
                                    let similarity_threshold=Some(0.7);
                                    let field_filter=Vec::new();
                                    let fields_hashset=HashSet::new();
                                    let mut recall_set=HashSet::new();


                                    let start_time = Instant::now();

                                    let result_object_vector = index_arc
                                    .search(
                                        query.to_string(),
                                        Some(query_vector.clone()),
                                        QueryType::Intersection,
                                        SearchMode::Vector { similarity_threshold , ann_mode: AnnMode::Similaritythreshold(0.0) },
                                        false,
                                        0,
                                        len,
                                        ResultType::Topk,
                                        false,
                                        field_filter.clone(),
                                        Vec::new(),
                                        Vec::new(),
                                        Vec::new(),
                                    )
                                    .await;
                                    if let Some(error) = &result_object_vector.error { eprintln!("search failed: {error}"); continue; }

                                    let search_time = start_time.elapsed().as_nanos() as i64;

                                    let mut min_cluster_score = f32::MAX;
                                    for (i, result) in result_object_vector.results.iter().enumerate() {
                                        let doc = index_arc.read().await.get_document(result.doc_id, false, &fields_hashset).await.ok();
                                        let title= if let Some(doc) = &doc { if let Some(title) = doc.get("title") { title.to_string() } else { "".to_string() } } else {"".to_string()};
                                        #[cfg(feature = "vb")]
                                        if result.cluster_score < min_cluster_score {
                                            min_cluster_score = result.cluster_score;
                                        }
                                        #[cfg(not(feature = "vb"))]
                                        {
                                        min_cluster_score=0.0;
                                        }

                                        recall_set.insert(result.doc_id);

                                        #[cfg(feature = "vb")]
                                        println!("Top {}: doc_id: {}, similarity: {}, lexical_score: {}, vector_score: {},  cluster_score: {}, shard_id: {}, level_id: {}, cluster_id: {}, field_id: {}, chunk_id: {}, source: {:?}, document: {} ", i+1, result.doc_id, result.score, result.lexical_score, result.vector_score, result.cluster_score, result.shard_id, result.level_id, result.cluster_id, result.field_id,result.chunk_id, result.source, title);
                                        #[cfg(not(feature = "vb"))]
                                        println!("Top {}: doc_id: {}, similarity: {},  document: {} ", i+1, result.doc_id, result.score, title);
                                    }

                                    let indexed_vector_count=index_arc.read().await.indexed_vector_count().await;
                                    let indexed_cluster_count=index_arc.read().await.indexed_cluster_count().await;

                                    println!("Search time: {} µs  result count {} result count total: {} clusters observed: {:.2}% ({} of {}) vectors observed: {:.2}% ({} of {})", (search_time/1000).to_formatted_string(&Locale::en), result_object_vector.results.len(), result_object_vector.result_count_total,
                                    (result_object_vector.observed_cluster_count as f64) / (indexed_cluster_count as f64) * 100.0,result_object_vector.observed_cluster_count.to_formatted_string(&Locale::en) , indexed_cluster_count.to_formatted_string(&Locale::en),
                                    (result_object_vector.observed_vector_count as f64) / (indexed_vector_count as f64) * 100.0,result_object_vector.observed_vector_count.to_formatted_string(&Locale::en) , indexed_vector_count.to_formatted_string(&Locale::en));
                                    println!("Minimum cluster score in results: {}", min_cluster_score);
                                    println!();


                                    let start_time = Instant::now();

                                    let result_object_vector = index_arc
                                    .search(
                                        query.to_string(),
                                        Some(query_vector.clone()),
                                        QueryType::Intersection,
                                        SearchMode::Vector { similarity_threshold , ann_mode: AnnMode::Similaritythreshold(min_cluster_score)},
                                        false,
                                        0,
                                        len,
                                        ResultType::Topk,
                                        false,
                                        field_filter.clone(),
                                        Vec::new(),
                                        Vec::new(),
                                        Vec::new(),
                                    )
                                    .await;
                                    if let Some(error) = &result_object_vector.error { eprintln!("search failed: {error}"); continue; }

                                    let mut recall_count=0;
                                    let search_time = start_time.elapsed().as_nanos() as i64;
                                    for (i, result) in result_object_vector.results.iter().enumerate() {
                                        let doc = index_arc.read().await.get_document(result.doc_id, false, &fields_hashset).await.ok();
                                        let title= if let Some(doc) = &doc { if let Some(title) = doc.get("title") { title.to_string() } else { "".to_string() } } else {"".to_string()};
                                        #[cfg(feature = "vb")]
                                        println!("Top {}: doc_id: {}, similarity: {}, lexical_score: {}, vector_score: {},  cluster_score: {}, shard_id: {}, level_id: {}, cluster_id: {}, field_id: {}, chunk_id: {}, source: {:?}, document: {} ", i+1, result.doc_id, result.score, result.lexical_score, result.vector_score, result.cluster_score, result.shard_id, result.level_id, result.cluster_id, result.field_id,result.chunk_id, result.source, title);
                                        #[cfg(not(feature = "vb"))]
                                        println!("Top {}: doc_id: {}, similarity: {},  document: {} ", i+1, result.doc_id, result.score, title);
                                        if recall_set.contains(&result.doc_id) { recall_count+=1; }
                                    }
                                    println!("Search time: {} µs  result count {} result count total: {} clusters observed: {:.2}% ({} of {}) vectors observed: {:.2}% ({} of {}) recall: {}%", (search_time/1000).to_formatted_string(&Locale::en), result_object_vector.results.len(), result_object_vector.result_count_total,
                                    (result_object_vector.observed_cluster_count as f64) / (index_arc.read().await.indexed_cluster_count as f64) * 100.0,result_object_vector.observed_cluster_count.to_formatted_string(&Locale::en) , index_arc.read().await.indexed_cluster_count.to_formatted_string(&Locale::en),
                                    (result_object_vector.observed_vector_count as f64) / (index_arc.read().await.indexed_vector_count as f64) * 100.0,result_object_vector.observed_vector_count.to_formatted_string(&Locale::en) , index_arc.read().await.indexed_vector_count.to_formatted_string(&Locale::en),
                                    (recall_count as f64) / (recall_set.len() as f64) * 100.0);
                                    println!();



                                    let start_time = Instant::now();
                                    let result_object_vector = index_arc
                                    .search(
                                        query.to_string(),
                                        Some(query_vector.clone()),
                                        QueryType::Intersection,
                                        SearchMode::Vector { similarity_threshold  , ann_mode: AnnMode::Nprobe(55) },
                                        false,
                                        0,
                                        len,
                                        ResultType::Topk,
                                        false,
                                        field_filter.clone(),
                                        Vec::new(),
                                        Vec::new(),
                                        Vec::new(),
                                    )
                                    .await;
                                    if let Some(error) = &result_object_vector.error { eprintln!("search failed: {error}"); continue; }

                                    let mut recall_count=0;
                                    let search_time = start_time.elapsed().as_nanos() as i64;
                                    for (i, result) in result_object_vector.results.iter().enumerate() {
                                        let doc = index_arc.read().await.get_document(result.doc_id, false, &fields_hashset).await.ok();
                                        let title= if let Some(doc) = &doc { if let Some(title) = doc.get("title") { title.to_string() } else { "".to_string() } } else {"".to_string()};

                                        #[cfg(feature = "vb")]
                                        println!("Top {}: doc_id: {}, similarity: {}, lexical_score: {}, vector_score: {},  cluster_score: {}, shard_id: {}, level_id: {}, cluster_id: {}, field_id: {}, chunk_id: {}, source: {:?}, document: {} ", i+1, result.doc_id, result.score, result.lexical_score, result.vector_score, result.cluster_score, result.shard_id, result.level_id, result.cluster_id, result.field_id,result.chunk_id, result.source, title);
                                        #[cfg(not(feature = "vb"))]
                                        println!("Top {}: doc_id: {}, similarity: {},  document: {} ", i+1, result.doc_id, result.score, title);

                                        if recall_set.contains(&result.doc_id) { recall_count+=1; }
                                    }
                                    println!("Search time: {} µs  result count {} result count total: {} clusters observed: {:.2}% ({} of {}) vectors observed: {:.2}% ({} of {}) recall: {}%", (search_time/1000).to_formatted_string(&Locale::en), result_object_vector.results.len(), result_object_vector.result_count_total,
                                    (result_object_vector.observed_cluster_count as f64) / (index_arc.read().await.indexed_cluster_count as f64) * 100.0,result_object_vector.observed_cluster_count.to_formatted_string(&Locale::en) , index_arc.read().await.indexed_cluster_count.to_formatted_string(&Locale::en),
                                    (result_object_vector.observed_vector_count as f64) / (index_arc.read().await.indexed_vector_count as f64) * 100.0,result_object_vector.observed_vector_count.to_formatted_string(&Locale::en) , index_arc.read().await.indexed_vector_count.to_formatted_string(&Locale::en),
                                    (recall_count as f64) / (recall_set.len() as f64) * 100.0);


                                }
                            }
                        }

                        "create" =>
                        {
                            println!("create demo api_key");
                            let mut apikey_list_mut = apikey_list_clone.write().await;
                            create_apikey_api(
                                &index_path,
                                &demo_api_key,
                                &mut apikey_list_mut,
                            );
                        }

                        "delete" =>
                        {
                            println!("delete demo api_key");
                            let apikey_hash = calculate_hash(&demo_api_key) as u128;
                            let mut apikey_list_mut = apikey_list_clone.write().await;
                            let _ = delete_apikey_api(&index_path, &mut apikey_list_mut, apikey_hash);
                            drop(apikey_list_mut);
                        },



                        "openapi" =>
                        {
                            generate_openapi();
                        },

                        "quit" =>
                        {
                            println!("Committing all indices ...");
                            let mut apikey_list_mut=apikey_list_clone.write().await;
                            for apikey in apikey_list_mut.iter_mut()
                            {
                                for index in apikey.1.index_list.iter_mut()
                                {
                                    index.1.close().await;
                                }
                            }
                            drop(apikey_list_mut);

                            println!("Server stopped by quit");
                            return;
                        },

                        "info" =>
                        {
                            let apikey_list_ref=apikey_list_clone.read().await;
                            for apikey in apikey_list_ref.iter() {
                                for index in apikey.1.index_list.iter() {
                                    display_index_info(index.1, "info", "0 s".to_string()).await;
                                }
                            }
                        }

                        "help" =>
                        {
                            println!("{}","Server console commands:".yellow());
                            println!();
                            println!("{:40} Create the demo API key manually to allow a subsequent custom create index via REST API.","create".green());
                            println!("{:40} Delete the demo API key and all its indices.","delete".green());
                            println!("{:40} Display current index information.","info".green());
                            println!("{:40} Create OpenAPI JSON file.","openapi".green());
                            println!("{:40} Stop the server.","quit".green());
                            println!("{:40} Show this help.","help".green());
                            println!();
                            println!("{} https://github.com/SeekStorm/SeekStorm/blob/main/seekstorm_server/README.md#console-commands","See details:".yellow());
                        },

                        &_ => {
                            println!("unknown command: {}",command);
                        }
                    }

                }

                }
            }
        } else {
            select! {
                recv(receiver_ctrl_c) -> _ => {
                    println!("Committing all indices ...");
                    let mut apikey_list_mut=apikey_list_clone.write().await;
                    for apikey in apikey_list_mut.iter_mut()
                    {
                        for index in apikey.1.index_list.iter_mut()
                        {
                            index.1.close().await;
                        }
                    }
                    drop(apikey_list_mut);

                    println!("Server stopped by Ctrl-C");
                    return;
                }
            }
        }
    }
}

