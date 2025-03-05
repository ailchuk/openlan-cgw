use cgw_common::{
    cgw_errors::{Error, Result},
    cgw_ucentral_parser::{
        cgw_ucentral_event_parse, cgw_ucentral_parse_connect_event, CGWUCentralCommandType,
        CGWUCentralEventType,
    },
};

use eui48::MacAddress;
use futures_util::{
    stream::{SplitSink, SplitStream},
    FutureExt, SinkExt, StreamExt,
};

use uuid::Uuid;
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
    time::{sleep, Duration, Instant},
};
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::{tungstenite::protocol::Message, WebSocketStream};
use tungstenite::Message::{Close, Ping, Text};

type SStream = SplitStream<WebSocketStream<TlsStream<TcpStream>>>;
type SSink = SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>;

use crate::proxy_connection_server::{
    ProxyConnectionServer,
    ProxyConnectionServerReqMsg,
};

#[derive(Debug, Clone)]
pub enum ProxyConnectionProcessorReqMsg {
    // We got green light from server to process this connection on
    // Upon creation, this conn processor is <assigned> to specific GID,
    // meaning in any replies sent from device it should include provided
    // GID (used as kafka key).
    AddNewConnectionAck(i32),
    AddNewConnectionShouldClose,
    ForeignConnection((String, u16)),
    // SinkRequestToDevice(CGWUCentralMessagesQueueItem),
    // Conn Server can request this specific Processor to change
    // it's internal GID value (infra list created - new gid,
    // infra list deleted - unassigned, e.g. GID 0).
    GroupIdChanged(i32),
}

#[derive(Debug)]
enum ProxyConnectionState {
    IsActive,
    IsForcedToClose,
    IsDead,
    #[allow(dead_code)]
    IsStale,
    ClosedGracefully,
}

#[derive(Debug, PartialEq)]
enum ProxyUCentralMessageProcessorState {
    Idle,
    ResultPending,
}

impl std::fmt::Display for ProxyUCentralMessageProcessorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyUCentralMessageProcessorState::Idle => write!(f, "Idle"),
            ProxyUCentralMessageProcessorState::ResultPending => write!(f, "ResultPending"),
        }
    }
}

pub struct ProxyConnectionProcessor {
    proxy_server: Arc<ProxyConnectionServer>,
    pub serial: MacAddress,
    pub addr: SocketAddr,
    pub group_id: i32,
}

impl ProxyConnectionProcessor {
    pub fn new(server: Arc<ProxyConnectionServer>, addr: SocketAddr) -> Self {
        let conn_processor: ProxyConnectionProcessor = ProxyConnectionProcessor {
            proxy_server: server.clone(),
            serial: MacAddress::default(),
            addr,
            group_id: 0,
        };

        conn_processor
    }

    pub async fn start(
        mut self,
        tls_stream: TlsStream<TcpStream>,
        client_cn: MacAddress,
    ) -> Result<()> {
        let ws_stream = tokio::select! {
            _val = tokio_tungstenite::accept_async(tls_stream) => {
                match _val {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to accept TLS stream from: {}! Reason: {}. Closing connection",
                               self.addr, e);
                        return Err(Error::ConnectionProcessor("Failed to accept TLS stream!"));
                    }
                }
            }
            // TODO: configurable duration (upon server creation)
            _val = sleep(Duration::from_millis(15000)) => {
                error!("Failed to accept TLS stream from: {}! Closing connection", self.addr);
                return Err(Error::ConnectionProcessor("Failed to accept TLS stream for too long"));
            }

        };

        let (sink, mut stream) = ws_stream.split();

        // check if we have any pending msgs (we expect connect at this point, protocol-wise)
        // TODO: rework to ignore any WS-related frames until we get a connect message,
        // however there's a caveat: we can miss some events logs etc from underlying device
        // rework should consider all the options
        let msg = tokio::select! {
            _val = stream.next() => {
                match _val {
                    Some(m) => m,
                    None => {
                        error!("No connect message received from: {}! Closing connection!", self.addr);
                        return Err(Error::ConnectionProcessor("No connect message received"));
                    }
                }
            }
            // TODO: configurable duration (upon server creation)
            _val = sleep(Duration::from_millis(30000)) => {
                error!("No message received from: {}! Closing connection", self.addr);
                return Err(Error::ConnectionProcessor("No message received for too long"));
            }
        };

        // we have a next() result, but it still may be underlying io error: check for it
        // break connection if we can't work with underlying ws connection (prot err etc)
        let message = match msg {
            Ok(m) => m,
            Err(e) => {
                error!(
                    "Established connection with device, but failed to receive any messages! Error: {e}"
                );
                return Err(Error::ConnectionProcessor(
                    "Established connection with device, but failed to receive any messages",
                ));
            }
        };

        debug!("Parse Connect Event");
        let evt = match cgw_ucentral_parse_connect_event(message.clone()) {
            Ok(event) => event,
            Err(e) => {
                error!(
                    "Failed to parse connect message from: {}! Error: {e}",
                    self.addr
                );
                return Err(Error::ConnectionProcessor(
                    "Failed to receive connect message",
                ));
            }
        };

        debug!(
            "Parse Connect Event done! Device serial: {}",
            evt.serial.to_hex_string()
        );

        self.serial = evt.serial;

        // TODO: we accepted tls stream and split the WS into RX TX part,
        // now we have to ASK proxy_connection_server's permission whether
        // we can proceed on with this underlying connection.
        // proxy_connection_server has an authoritative decision whether
        // we can proceed.
        debug!("Sending ACK request for device serial: {}", self.serial);
        let orig_connect_msg = message.into_text().unwrap_or_default();
        let (mbox_tx, mut mbox_rx) = unbounded_channel::<ProxyConnectionProcessorReqMsg>();
        let msg = ProxyConnectionServerReqMsg::AddNewConnection(
            evt.serial,
            self.addr,
            mbox_tx,
            orig_connect_msg,
        );
        self.proxy_server
            .enqueue_mbox_message_to_proxy_server(msg)
            .await;

        let ack = mbox_rx.recv().await;
        debug!("Got ACK response for device serial: {}", self.serial);
        if let Some(m) = ack {
            match m {
                ProxyConnectionProcessorReqMsg::AddNewConnectionAck(gid) => {
                    debug!(
                        "WebSocket connection established! Address: {}, serial: {} gid {gid}",
                        self.addr, evt.serial
                    );
                    self.group_id = gid;
                }
                _ => {
                    return Err(Error::ConnectionProcessor(
                        "Unexpected response from server! Expected: ACK/NOT ACK",
                    ));
                }
            }
        } else {
            info!("Connection server declined connection! WebSocket connection for address: {}, serial: {} cannot be established!",
                  self.addr, evt.serial);
            return Err(Error::ConnectionProcessor("WebSocket connection declined"));
        }

        // self.process_connection(stream, sink, mbox_rx).await;

        Ok(())
    }

    async fn process_connection(
        mut self,
        mut stream: SStream,
        mut sink: SSink,
        mut mbox_rx: UnboundedReceiver<ProxyConnectionProcessorReqMsg>,
    ) {

    }
}