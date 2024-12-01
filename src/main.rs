use std::io::Cursor;
use whatlang::detect;
use syllarust::estimate_syllables;
use std::fs::OpenOptions;
use std::io::Write;

use atrium_api::{
    app::bsky::{
        feed::{post, like, repost},
        graph::follow,
    },
    com::atproto::sync::subscribe_repos::Commit,
};
use futures_util::StreamExt;

use ipld_core::ipld::Ipld;
use native_tls::TlsConnector;
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
    Connector,
};
use tracing::{error, info};

const FIREHOSE_URL: &str = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
const USER_AGENT: &str =
    "bsky-firehose-listener (https://github.com/angeloanan/bsky-firehose-listener)";

fn is_english(text: &str) -> bool {
    detect(text).map_or(false, |info| info.lang() == whatlang::Lang::Eng)
}

fn is_haiku(text: &str) -> bool {
    let lines: Vec<String> = if text.contains('\n') {
        text.lines().map(|s| s.to_string()).collect()
    } else {
        text.split_whitespace()
            .collect::<Vec<&str>>()
            .chunks(5)
            .map(|chunk| chunk.join(" "))
            .collect::<Vec<String>>()
    };

    if lines.len() != 3 {
        return false;
    }

    let syllables: Vec<usize> = lines.iter().map(|line| estimate_syllables(&line)).collect();
    syllables == vec![5, 7, 5]
}

fn save_haiku_to_file(haiku: &str, cid: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("haikus.txt")?;
    writeln!(file, "CID: {}\n{}\n", cid, haiku)?;
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let mut firehose_request = FIREHOSE_URL.into_client_request().unwrap();
    firehose_request
        .headers_mut()
        .append("User-Agent", HeaderValue::from_str(USER_AGENT).unwrap());
    let (mut stream, _response) = tokio_tungstenite::connect_async_tls_with_config(
        firehose_request,
        None,
        true,
        Some(Connector::NativeTls(TlsConnector::new().expect(
            "Unable to use Native TLS. Does your system have it installed?",
        ))),
    )
    .await
    .unwrap();
    info!("Connected to Firehose.");

    while let Some(msg) = stream.next().await {
        if let Err(e) = msg {
            info!("Error connecting to Firehose: {:?}", e);
            continue;
        }

        let msg = msg.unwrap();
        match msg {
            Message::Binary(data) => {
                tokio::task::spawn(async move {
                    let mut cursor = Cursor::new(data.as_slice());
                    serde_ipld_dagcbor::from_reader::<Ipld, _>(&mut cursor)
                        .expect_err("Somehow bsky only sends 1 frame.");
                    let (metadata, data) = data.split_at(cursor.position() as usize);

                    let Ipld::Map(map) = serde_ipld_dagcbor::from_slice::<Ipld>(metadata)
                        .expect("Valid data turns out to be invalid")
                    else {
                        error!("Expected a map, got something else: {:?}", data);
                        return;
                    };

                    let Ipld::Integer(op_id) =
                        map.get("op").expect("Malformed frame, \"op\" is missing")
                    else {
                        error!("Malformed bsky data. Expected \"op\" to be an integer, got something else: {:?}", data);
                        return;
                    };

                    if *op_id == -1 {
                        error!("Bluesky sent op=-1 (error). Ignoring message.");
                        return;
                    }

                    let Ipld::String(message) =
                        map.get("t").expect("Malformed frame, \"t\" is missing")
                    else {
                        error!("Malformed bsky data. Expected \"t\" to be a string, got something else: {:?}", data);
                        return;
                    };

                    if message != "#commit" {
                        return;
                    }

                    let commit = serde_ipld_dagcbor::from_slice::<Commit>(data)
                        .expect("Malformed bsky \"#commit\" data");

                    let (items, _header) =
                        rs_car::car_read_all(&mut commit.blocks.as_slice(), true)
                            .await
                            .expect("CAR file is invalid");
                    let items_iter = items.iter();
                    for operation in &commit.ops {
                        if operation.action != "create" {
                            continue;
                        }

                        let Some((_header, data)) = items_iter.clone().find(|(cid, _value)| {
                            Some(cid.to_string())
                                == operation.cid.as_ref().map(|cid| cid.0.to_string())
                        }) else {
                            error!("Could not find block for CID {:?}", operation.cid);
                            continue;
                        };

                        match operation.path.as_str() {
                            path if path.starts_with("app.bsky.feed.post") => {
                                if let Ok(record) = serde_ipld_dagcbor::from_reader::<post::Record, _>(data.as_slice()) {
                                    //do the things
                                    if is_english(&record.text) && is_haiku(&record.text) {
                                        info!("New haiku found:");
                                        for line in record.text.lines() {
                                            info!("{}", line);
                                        }
                                        if let Err(e) = save_haiku_to_file(&record.text, &operation.cid.as_ref().unwrap().0.to_string()) {
                                            error!("Failed to save haiku: {:?}", e);
                                        } else {
                                            info!("Haiku saved to file");
                                        }
                                    }
                                    info!("New post: {:?} - {}", operation.cid, record.text);
                                }
                            },
                            path if path.starts_with("app.bsky.feed.like") => {
                                if let Ok(record) = serde_ipld_dagcbor::from_reader::<like::Record, _>(data.as_slice()) {
                                    info!("New like: {:?} - Subject: {}", operation.cid, record.subject.uri);
                                }
                            },
                            path if path.starts_with("app.bsky.feed.repost") => {
                                if let Ok(record) = serde_ipld_dagcbor::from_reader::<repost::Record, _>(data.as_slice()) {
                                    info!("New repost: {:?} - Subject: {}", operation.cid, record.subject.uri);
                                }
                            },
                            path if path.starts_with("app.bsky.graph.follow") => {
                                if let Ok(record) = serde_ipld_dagcbor::from_reader::<follow::Record, _>(data.as_slice()) {
                                    info!("New follow: {:?} - Subject: {:?}", operation.cid, record.subject);
                                }
                            },
                            _ => {
                                info!("Unknown event type: {}", operation.path);
                            }
                        }
                    }
                });
            }
            Message::Close(_) => {
                info!("Firehose disconnected us.");
            }
            _ => {}
        }
    }

    info!("Disconnected from Firehose.");
}
