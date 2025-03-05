use cgw_common::{
    cgw_errors::{Error, Result},
    cgw_tls::cgw_tls_get_cn_from_stream,
    cgw_ucentral_parser::{
        CGWUCentralConfigValidators,
    },
    cgw_app_args::AppArgs
};

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    net::TcpStream,
    runtime::Runtime,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    time::{sleep, Duration},
};
use tokio::time;

use eui48::MacAddress;

use crate::proxy_runtime::{proxy_get_runtime, ProxyRuntimeType};
use crate::proxy_connection_processor::{
    ProxyConnectionProcessor,
    ProxyConnectionProcessorReqMsg,
};

type ProxyConnmapType =
    Arc<RwLock<HashMap<MacAddress, UnboundedSender<ProxyConnectionProcessorReqMsg>>>>;

#[derive(Debug)]
struct ProxyConnMap {
    map: ProxyConnmapType,
}

impl ProxyConnMap {
    pub fn new() -> Self {
        let hash_map: HashMap<MacAddress, UnboundedSender<ProxyConnectionProcessorReqMsg>> =
            HashMap::new();
        let map: Arc<RwLock<HashMap<MacAddress, UnboundedSender<ProxyConnectionProcessorReqMsg>>>> =
            Arc::new(RwLock::new(hash_map));

        ProxyConnMap { map }
    }
}

type ProxyConnectionServerMboxRx = UnboundedReceiver<ProxyConnectionServerReqMsg>;
type ProxyConnectionServerMboxTx = UnboundedSender<ProxyConnectionServerReqMsg>;

// The following pair used internally by server itself to bind
// Processor's Req/Res
#[derive(Debug)]
pub enum ProxyConnectionServerReqMsg {
    // Connection-related messages
    AddNewConnection(
        MacAddress,
        SocketAddr,
        UnboundedSender<ProxyConnectionProcessorReqMsg>,
        String,
    ),
    ConnectionClosed(MacAddress),
}

pub struct ProxyConnectionServer {
    local_cgw_id: i32,
    // ProxyConnectionServer write into this mailbox,
    // and other corresponding Server task Reads RX counterpart
    mbox_internal_tx: ProxyConnectionServerMboxTx,

    // Object that owns underlying mac:connection map
    connmap: ProxyConnMap,

    // Runtime that schedules all the WSS-messages related tasks
    wss_rx_tx_runtime: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling internal mbox:
    // ACK/nACK connection, handle duplicates (clone/open) etc.
    mbox_internal_runtime_handle: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling (relaying) msgs:
    // relay-task is spawned inside it, and the produced stream of
    // remote-cgw messages is being relayed inside this context
    mbox_relay_msg_runtime_handle: Arc<Runtime>,

    // Dedicated runtime (threadpool) for handling disconnected devices message queue
    // Iterate over list of disconnected devices - dequeue aged messages
    queue_timeout_handle: Arc<Runtime>,

    // UCentral command messages validators
    // for access points and switches
    config_validator: CGWUCentralConfigValidators,

    pub infras_capacity: i32,
    pub local_shard_partition_key: RwLock<Option<String>>,
    pub last_update_timestamp: RwLock<i64>,
}


impl ProxyConnectionServer {
    pub async fn new(app_args: &AppArgs) -> Result<Arc<Self>> {
        let wss_runtime_handle = match proxy_get_runtime(ProxyRuntimeType::WssRxTx) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        ProxyRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    ProxyRuntimeType::WssRxTx
                )));
            }
        };

        let internal_mbox_runtime_handle = match proxy_get_runtime(ProxyRuntimeType::MboxInternal) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        ProxyRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    ProxyRuntimeType::WssRxTx
                )));
            }
        };

        let relay_msg_mbox_runtime_handle = match proxy_get_runtime(ProxyRuntimeType::MboxRelay) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        ProxyRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    ProxyRuntimeType::WssRxTx
                )));
            }
        };

        let queue_timeout_handle = match proxy_get_runtime(ProxyRuntimeType::QueueTimeout) {
            Ok(ret_runtime) => match ret_runtime {
                Some(runtime) => runtime,
                None => {
                    return Err(Error::ConnectionServer(format!(
                        "Failed to find runtime type {:?}",
                        ProxyRuntimeType::WssRxTx
                    )));
                }
            },
            Err(e) => {
                return Err(Error::ConnectionServer(format!(
                    "Failed to get runtime type {:?}! Error: {e}",
                    ProxyRuntimeType::WssRxTx
                )));
            }
        };

        let (internal_tx, internal_rx) = unbounded_channel::<ProxyConnectionServerReqMsg>();

        // TODO: proper fix.
        // Ugly W/A for now;
        // The reason behind this change (W/A), is that underlying validator
        // uses sync call, which panics (due to it being called in async
        // context).
        // The proper fix would to be refactor all constructors to be sync,
        // but use spawn_blocking where needed in contexts that rely on the
        // underlying async calls.
        let app_args_clone = app_args.validation_schema.clone();
        let get_config_validator_fut =
            tokio::task::spawn_blocking(move || CGWUCentralConfigValidators::new(app_args_clone));
        let config_validator = match get_config_validator_fut.await {
            Ok(res) => match res {
                Ok(validator) => validator,
                Err(e) => {
                    error!(
                        "Can't create CGW Connection server: Config validator create failed: {e}"
                    );

                    return Err(Error::ConnectionServer(format!(
                        "Can't create CGW Connection server: Config validator create failed: {e}",
                    )));
                }
            },
            Err(e) => {
                error!("Failed to retrieve json config validators! Error: {e}");
                return Err(Error::ConnectionServer(format!(
                    "Failed to retrieve json config validators! Error: {e}"
                )));
            }
        };

        let server = Arc::new(ProxyConnectionServer {
            local_cgw_id: app_args.cgw_id,
            connmap: ProxyConnMap::new(),
            wss_rx_tx_runtime: wss_runtime_handle,
            mbox_internal_runtime_handle: internal_mbox_runtime_handle,
            mbox_internal_tx: internal_tx,
            queue_timeout_handle,
            mbox_relay_msg_runtime_handle: relay_msg_mbox_runtime_handle,
            config_validator,
            infras_capacity: app_args.cgw_group_infras_capacity,
            local_shard_partition_key: RwLock::new(None),
            last_update_timestamp: RwLock::new(0i64),
        });

        let server_clone = server.clone();
        // Task for processing mbox_internal_rx, task owns the RX part
        server.mbox_internal_runtime_handle.spawn(async move {
            server_clone.process_internal_mbox(internal_rx).await;
        });

        // let server_clone = server.clone();
        // server.queue_timeout_handle.spawn(async move {
        //     server_clone.start_queue_timeout_manager().await;
        // });

        Ok(server)
    }

    pub async fn enqueue_mbox_message_to_proxy_server(&self, req: ProxyConnectionServerReqMsg) {
        if let Err(e) = self.mbox_internal_tx.send(req) {
            error!("Failed to send message to Proxy server (internal)! Error: {e}");
        }
    }

    pub async fn ack_connection(
        self: Arc<Self>,
        socket: TcpStream,
        tls_acceptor: tokio_rustls::TlsAcceptor,
        addr: SocketAddr,
    ) {
        // Only ACK connection. We will either drop it or accept it once processor starts
        // (we'll handle it via "mailbox" notify handle in process_internal_mbox)
        let server_clone = self.clone();

        self.wss_rx_tx_runtime.spawn(async move {
            // Accept the TLS connection.
            let (client_cn, tls_stream) = match tls_acceptor.accept(socket).await {
                Ok(stream) => match cgw_tls_get_cn_from_stream(&stream).await {
                    Ok(cn) => (cn, stream),
                    Err(e) => {
                        error!("Failed to read client CN! Error: {e}");
                        return;
                    }
                },
                Err(e) => {
                    error!("Failed to accept connection: Error {e}");
                    return;
                }
            };

            let conn_processor = ProxyConnectionProcessor::new(server_clone, addr);
            if let Err(e) = conn_processor
                .start(tls_stream, client_cn)
                .await
            {
                error!("Failed to start connection processor! Error: {e}");
            }
        });
    }

    async fn process_internal_mbox(self: Arc<Self>, mut rx_mbox: ProxyConnectionServerMboxRx) {
        debug!("process_internal_mbox entry");

        let buf_capacity = 1000;
        let mut buf: Vec<ProxyConnectionServerReqMsg> = Vec::with_capacity(buf_capacity);
        let mut num_of_msg_read = 0;

        loop {
            if num_of_msg_read < buf_capacity {
                // Try to recv_many, but don't sleep too much
                // in case if no messaged pending and we have
                // TODO: rework?
                // Currently recv_many may sleep if previous read >= 1,
                // but no new messages pending
                let rd_num = tokio::select! {
                    v = rx_mbox.recv_many(&mut buf, buf_capacity - num_of_msg_read) => {
                        v
                    }
                    _v = sleep(Duration::from_millis(10)) => {
                        0
                    }
                };
                num_of_msg_read += rd_num;

                // We read some messages, try to continue and read more
                // If none read - break from recv, process all buffers that've
                // been filled-up so far (both local and remote).
                // Upon done - repeat.
                if rd_num >= 1 {
                    if num_of_msg_read < 100 {
                        continue;
                    }
                } else if num_of_msg_read == 0 {
                    continue;
                }
            }

            let mut connmap_w_lock = self.connmap.map.write().await;

            while !buf.is_empty() {
                let msg = buf.remove(0);

                if let ProxyConnectionServerReqMsg::AddNewConnection(
                    device_mac,
                    ip_addr,
                    conn_processor_mbox_tx,
                    orig_connect_message,
                ) = msg
                {
                    // if connection is unique: simply insert new conn
                    //
                    // if duplicate exists: notify server about such incident.
                    // it's up to server to notify underlying task that it should
                    // drop the connection.
                    // from now on simply insert new connection into hashmap and proceed on
                    // processing it.
                    if let Some(c) = connmap_w_lock.remove(&device_mac) {
                        tokio::spawn(async move {
                            warn!("Duplicate connection (mac: {}) detected! Closing OLD connection in favor of NEW!", device_mac);
                            let msg: ProxyConnectionProcessorReqMsg =
                                ProxyConnectionProcessorReqMsg::AddNewConnectionShouldClose;
                            if let Err(e) = c.send(msg) {
                                warn!("Failed to send notification about duplicate connection! Error: {e}")
                            }
                        });
                    }

                    // clone a sender handle, as we still have to send ACK back using underlying
                    // tx mbox handle
                    let conn_processor_mbox_tx_clone = conn_processor_mbox_tx.clone();

                    info!(
                        "Connection map: connection with {} established, new num_of_connections: {}",
                        device_mac,
                        connmap_w_lock.len() + 1
                    );

                    connmap_w_lock.insert(device_mac, conn_processor_mbox_tx);
                } else if let ProxyConnectionServerReqMsg::ConnectionClosed(device_mac) = msg {
                    let mut device_group_id: i32 = 0;
                    info!(
                        "Connection map: removed {} serial from connmap, new num_of_connections: {}",
                        device_mac,
                        connmap_w_lock.len() - 1
                    );
                    connmap_w_lock.remove(&device_mac);
                }
            }

            buf.clear();
            num_of_msg_read = 0;
        }
    }
}