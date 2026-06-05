mod vcpreciever;

use std::cmp;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{io, str::FromStr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UdpSocket, TcpSocket};
use tokio::time::{Interval, interval};
use std::collections::{HashMap, btree_map};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::vcpreciever::{VcpReceiver, VcpReceptionState};

#[derive(Debug)]
pub enum VcpMessage {
    Call {
        ip: String,
        port: u16,
        mimetype: String,
        username: String,
    },
    AcceptCall {
        ip: String,
        port: u16,
        mimetype: String,
        username: String,
    },
    DeclineCall {
        ip: String,
        port: u16,
        mimetype: String,
        username: String,
    },
    Packet {
        packet_nr: u64,
        timestamp: i64,
        data_len: u16,
        data: Vec<u8>,
        raw_bytes: Vec<u8>
    },
}

struct JitterBuffer {
    buffer: BTreeMap<u64, Vec<u8>>,
    last_popped: u64,
    highest_packet_nr: u64,
    total_received_packets: u64,
    snapshots: VecDeque<NetworkSnapshot>,
}

struct NetworkSnapshot {
    packets_received: u64,
    highest_packet_nr: u64
}

struct Connection {
    jitter_buffer: JitterBuffer,
    latency: i16,
    packet_loss: f64,
}

impl Connection {
    fn new() -> Self {
        Self {
            jitter_buffer: JitterBuffer::new(),
            latency: 0, 
            packet_loss: 0.0
        }

    }
}

impl NetworkSnapshot {
    fn new(packets_received: u64, highest_packet_nr: u64) -> Self {
        Self {
            packets_received: packets_received,
            highest_packet_nr: highest_packet_nr,
        }
    }
}



impl JitterBuffer {
    fn new() -> Self  {
        Self {
            buffer: BTreeMap::new(),
            last_popped: 0,
            highest_packet_nr: 0,
            snapshots: VecDeque::with_capacity(10),
            total_received_packets: 0,
        }
    }


    fn calculate_packet_loss(&self) -> f64 {
        if self.snapshots.len() < 2 {return 0.0};
        let Some(current_snapshot) = self.snapshots.back() else {return 0.0};
        let highest = current_snapshot.highest_packet_nr;
        let Some(oldest_snapshot) = self.snapshots.front() else {return 0.0};
        let lowest = oldest_snapshot.highest_packet_nr;

        let expected_packets = highest - lowest + 1;
        let received_packets = current_snapshot.packets_received - oldest_snapshot.packets_received as u64;
        if received_packets < expected_packets {
            let diff = expected_packets - received_packets;
            let packet_loss_ratio = (diff as f64) / (expected_packets as f64);
            return packet_loss_ratio * 100.0;
        }
        return 100.0;
    }

    fn add_packet(&mut self, packet_nr: u64, packet: &Vec<u8>,) {
        if packet_nr > self.highest_packet_nr  {
            self.highest_packet_nr = packet_nr
        }
        if !self.buffer.contains_key(&packet_nr) {
            self.total_received_packets += 1;
        }
        self.buffer.insert(packet_nr, packet.to_vec());

    }

    fn take_snapshot(&mut self) {
        if self.snapshots.len() == 10 {
            self.snapshots.pop_front();
        }
        let snapshot = NetworkSnapshot::new(self.total_received_packets, self.highest_packet_nr);
        self.snapshots.push_back(snapshot);
    }
}

async fn udp_thread(
    sock: Arc<UdpSocket>,
    udp_map_clone: Arc<Mutex<HashMap<String, Connection>>>
) {
    let mut buf = [0; 1500]; 
    println!("UDP task running...");

    let mut receivers: HashMap<String, VcpReceiver> = HashMap::new();

    let mut callpending: Option<SocketAddr> = None;

    loop {
        match sock.recv_from(&mut buf).await {
            Ok((amnt_read, addr)) => {
                let r = receivers.get_mut(&addr.ip().to_string());
                match r {
                    None => {
                        let r = VcpReceiver::new(buf[0..amnt_read].to_vec());
                        receivers.insert(addr.ip().to_string(), r);
                    }
                    Some(r) => {
                        r.feed(buf[0..amnt_read].to_vec());
                    }
                }

                let r = receivers.get_mut(&addr.ip().to_string()).unwrap();
                while *r.get_state() == VcpReceptionState::Done {
                    let action = r.get_action().as_str();
                    let args = r.get_args();
                    match action {
                        "CALL" => {
                            let ip = args[0].clone();
                            let port = args[1].clone();
                            let mimetype = args[2].clone();
                            let username = args[3].clone();
                            let text = format!("Incoming Call from {ip} port {port} with mimetype {mimetype} and username {username}");
                            unsafe { std::env::set_var("VCD_SOCKET", "127.0.0.1") };
                            unsafe { std::env::set_var("VCD_SOCKET_PORT", "8432") };
                            match std::process::Command::new("start-vcp-client")
                                .args(["CALL", ip.as_str(), port.to_string().as_str(), mimetype.as_str(), username.as_str()])
                                .spawn() {
                                    Err(e) => eprintln!("Failed to start client: {}", e),
                                    Ok(c) => eprintln!("Started client"),
                                }

                            callpending = Some(addr);
                            if let Err(e) = sock.send_to(text.as_bytes(), addr).await {
                                eprintln!("Failed to send Call response: {}", e);
                            }
                            eprintln!("{:?}", r.get_args());
                        }

                        "ACCEPTCALL" => {
                            let ip = args[0].clone();
                            let port = args[1].clone();
                            let mime = args[2].clone();
                            let user = args[3].clone();
                            if let Some(..) = callpending && let Ok(mut cmap) = udp_map_clone.lock() {
                                cmap.insert(callpending.unwrap().ip().to_string(), Connection::new());
                                callpending = None;
                            } else {
                                eprintln!("Failed to lock");
                            }
                        }

                        "PACKET" => {
                            let mut cmap = udp_map_clone.lock().unwrap();
                            let packet_nr = r.get_packet_nr();
                            let raw_bytes = r.get_result();
                            let timestamp = r.get_packet_ts() as i64;

                            if let Some(conn) = cmap.get_mut(&addr.ip().to_string()) {
                                //ignore packet if old
                                if packet_nr >= conn.jitter_buffer.last_popped {
                                    conn.jitter_buffer.add_packet(packet_nr, &raw_bytes);
                                    let cur_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("System time broke").as_millis() as i64;
                                    let latency = cur_time - timestamp;
                                    conn.latency = latency as i16;
                                }
                            } else {
                                //placeholder for later testing purposes
                                panic!("Sending packets before connection intialized");
                            }
                        }

                        _ => eprintln!("Invalid action {action}"),
                    }
                    r.reset();
                    r.feed(vec![]);
                }
            }
            Err(err) => eprintln!("Socket Receive Error: {}", err)
        };
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:7000").await?;
    let shared_socket = Arc::new(sock);
    eprintln!("Listening...");
    let connections_map = Arc::new(Mutex::new(HashMap::<String, Connection>::new()));

    let udp_handle = tokio::spawn(udp_thread(Arc::clone(&shared_socket), Arc::clone(&connections_map)));

    //for taking snapshots of connections
    let cmap_clone = Arc::clone(&connections_map);
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            timer.tick().await;
            if let Ok(mut cmap) = cmap_clone.lock() {
                for (_, conn) in cmap.iter_mut() {
                    conn.jitter_buffer.take_snapshot();
                    let packet_loss = conn.jitter_buffer.calculate_packet_loss();
                    conn.packet_loss = packet_loss;
                }
            }
        }
    });

    let tcp_sock = TcpSocket::new_v4().unwrap();
    tcp_sock.bind("127.0.0.1:8432".parse().unwrap())?;
    let tcp = tcp_sock.listen(1)?;
    let (conn, addr) = tcp.accept().await?;
    let(read_half, write_half) = conn.into_split();
    let shared_tcp_writer = Arc::new(tokio::sync::Mutex::new(write_half));
    let cmap_clone = Arc::clone(&connections_map);
    let udp_clone = shared_socket.clone();
    tokio::spawn(async move {
        let sock = udp_clone;
        let mut tcp_reader = read_half;
        let mut buf = [0; 1024];
        let mut receiver = VcpReceiver::new(vec![]);
        let mut has_responded_to_call = false;

        /*********************************************************************
        *                  <del>                                             *
        *                      START WITH A DUMMY ADDRESS                    *
        *                      INCREDIBLY IMPORTANT                          *
        *                  </del>                                            *
        *                  THE DUMMY ADDRESS HAS BEEN DELETED                *
        *                  PLEASE SEE SECTION 3.4-A AS TO WHY THIS IS        *
        *********************************************************************/
        loop {
            if let Ok(amt) = tcp_reader.read(&mut buf).await {
                if receiver.get_state() != &VcpReceptionState::Done && !has_responded_to_call{
                    receiver.feed(buf[0..amt].to_vec());
                } else if !has_responded_to_call{
                    has_responded_to_call = true;
                    let args = receiver.get_args();
                    match receiver.get_action().as_str() {
                        "ACCEPTCALL" => {
                            let ip = args[0].clone();
                            let port = args[1].clone();
                            let mimetype = args[2].clone();
                            let username = args[3].clone();
                            let response = format!("ACCEPTCALL {} {} {} \"{}\"\r\n", ip, port, mimetype, username);

                            match SocketAddr::from_str(&format!("{}:{}", ip, port).to_string()) {
                                Ok(addr) => {
                                    if let Err(e) = sock.send_to(response.as_bytes(), addr).await {
                                        eprintln!("Failed to send resp {}", e);
                                    }
                                },
                                Err(e) => {
                                    eprintln!("Failed to convert ip {}", e);
                                }
                            };
                            eprintln!("{} picked up {}:{}'s call with {}", username, ip, port, mimetype)
                        },
                        "DECLINECALL" => {
                            let ip = args[0].clone();
                            let port = args[1].clone();
                            let mimetype = args[2].clone();
                            let username = args[3].clone();
                            eprintln!("{} declined to pick up {}:{}'s call", username, ip, port)
                        },
                        _ => eprintln!("Invalid action {}", receiver.get_action().as_str()),
                    }

                    receiver.reset();
                } else {

                    let keys: Vec<String> = {
                        if let Ok(guard) = cmap_clone.lock() {
                            guard.keys().cloned().collect()
                        } else {
                            eprintln!("DROPPED PACKET");
                            //this will drop a packet
                            vec![]
                        }
                    };

                    for connection in keys {
                        if let Err(e) = sock.send_to(&buf[..amt], format!("{connection}:7000")).await {
                            eprintln!("Failed to forward packet to {}: {}", connection, e);
                        }
                    }
                }
            }
        }
    });
    let cmap_clone = connections_map.clone();
    let value = shared_tcp_writer.clone();
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_millis(40));
       loop {
            timer.tick().await;
            let mut packets_to_send: Vec<Vec<u8>> = vec![];
            if let Ok(mut cmap) = cmap_clone.lock() {
                for (_, conn) in cmap.iter_mut() {
                    if let Some((_nr, packet)) = conn.jitter_buffer.buffer.pop_first() {
                        packets_to_send.push(packet);
                    }
                }
            }
            for packet in packets_to_send {
                let mut tcp_conn = value.lock().await;
                if let Err(e) = tcp_conn.write_all(&packet).await {
                    eprintln!("Failed to write packet: {}", e);
                }
            }
        }

    });
    
    std::future::pending::<()>().await;
    Ok(())
}
