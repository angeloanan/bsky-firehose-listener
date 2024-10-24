use std::io::Cursor;

use atrium_api::{
    app::bsky::feed::post,
    com::atproto::sync::subscribe_repos::Commit,
    types::{CidLink, Collection},
};
use futures_util::{SinkExt, StreamExt};

use ipld_core::ipld::Ipld;
use native_tls::TlsConnector;
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
    Connector,
};
use tracing::{debug, error, info};

const FIREHOSE_URL: &str = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
const USER_AGENT: &str =
    "bsky-firehose-listener (https://github.com/angeloanan/bsky-firehose-listener)";

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
                // Handle each binary data in a separate task
                tokio::task::spawn(async move {
                    // On a single WS binary data, message will contain two ipld dagcbor frames:
                    // The first frame is the type of message (metadata)
                    // The second frame is the actual data of the message
                    //
                    // We need to split the data into two parts but don't know the size of each
                    // frame ahead of time. For now, we'll just try to parse the data as-is; We'll
                    // exploit how std::io::Cursor's position will be updated when we read from it.
                    let buf = data.clone();
                    let mut cursor = Cursor::new(data.as_slice());
                    serde_ipld_dagcbor::from_reader::<Ipld, _>(&mut cursor)
                        .expect_err("Somehow bsky only sends 1 frame.");
                    let (metadata, data) = data.split_at(cursor.position() as usize);

                    // Parse the metadata half
                    let Ipld::Map(map) = serde_ipld_dagcbor::from_slice::<Ipld>(metadata)
                        .expect("Valid data turns out to be invalid")
                    else {
                        error!("Expected a map, got something else: {:?}", data);
                        return;
                    };

                    // Parse `op`
                    //  1 = Message
                    // -1 = Error
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

                    // Parse `t`: https://github.com/bluesky-social/atproto/blob/c307a75db11503eedf743c01e62f90413f07fe2a/lexicons/com/atproto/sync/subscribeRepos.json#L20-L27
                    let Ipld::String(message) =
                        map.get("t").expect("Malformed frame, \"t\" is missing")
                    else {
                        error!("Malformed bsky data. Expected \"t\" to be a string, got something else: {:?}", data);
                        return;
                    };

                    // Only going to parse #commit
                    // info!("Received message from Firehose: {:?}", message);
                    if message != "#commit" {
                        return;
                    }

                    // Parse the data half
                    let commit = serde_ipld_dagcbor::from_slice::<Commit>(data)
                        .expect("Malformed bsky \"#commit\" data");

                    // Parse CAR file
                    let (items, _header) =
                        rs_car::car_read_all(&mut commit.blocks.as_slice(), true)
                            .await
                            .expect("CAR file is invalid");
                    let items_iter = items.iter();
                    for operation in &commit.ops {
                        // Only parse CREATE action
                        if operation.action != "create" {
                            continue;
                        }

                        // Only parse post
                        if !operation.path.starts_with("app.bsky.feed.post") {
                            // info!("Skipping non-post: {:?}", operation.path);
                            continue;
                        }

                        let Some((_header, data)) = items_iter.clone().find(|(cid, _value)| {
                            Some(cid.to_string())
                                == operation.cid.as_ref().map(|cid| cid.0.to_string())
                        }) else {
                            error!("Could not find block for CID {:?}", operation.cid);
                            continue;
                        };

                        let record =
                            serde_ipld_dagcbor::from_reader::<post::Record, _>(data.as_slice())
                                .expect("Malformed bsky \"#commit\" data");
                        info!(
                            "{} {:?} - {}",
                            operation.action.to_uppercase(),
                            operation.cid,
                            record.text
                        )
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
