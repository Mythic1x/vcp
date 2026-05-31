mod vcpreciever;

use std::cmp;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{io, str::FromStr};
use tokio::io::AsyncReadExt;
use tokio::net::{UdpSocket, TcpSocket};
use tokio::time::{Interval, interval};
use std::collections::{HashMap, btree_map};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::vcpreciever::{VcpReceiver, VcpReceptionState};

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
    },
}

struct JitterBuffer {
    buffer: BTreeMap<u64, Vec<u8>>,
    last_popped: u64,
    highest_packet_nr: u64,
    total_received_packets: u64,
    //list of snapshots taken every 10 seconds up to 60
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
        let diff = expected_packets - received_packets;
        let packet_loss_ratio = (diff as f64) / (expected_packets as f64);
        return packet_loss_ratio * 100.0;
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





impl VcpMessage {
    fn parse(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.starts_with(b"PACKET/")  {
            if bytes.len() < 15 {
                return Err("Malformed Packet: Missing Data");
            }
            let packet_nr_bytes = &bytes[7..15];
            let packet_nr = u64::from_be_bytes(packet_nr_bytes.try_into().map_err(|_| "Malformed packet found when parsing packet number")?);
            let timestamp_bytes = &bytes[15..23];
            let timestamp = i64::from_be_bytes(timestamp_bytes.try_into().map_err(|_| "Malformed packet found when parsing timestamp")?);
            let data_len_bytes = &bytes[23..25]; 
            let data_len = u16::from_be_bytes(data_len_bytes.try_into().map_err(|_| "Malformed packet found when parsing data length")?);
            let data_len_us = data_len as usize;
            if bytes.len() < 25 + data_len_us {
                return Err("Malformed packet: data length exceeds packet size");
            }
            let data = bytes[17..17 + data_len_us].to_vec();
            return Ok(VcpMessage::Packet { packet_nr, timestamp, data_len, data });
        } else {
            let text = str::from_utf8(bytes).map_err(|_| "Error converting byte array into string")?;
            let text = text.strip_suffix("\r\n").unwrap_or(text);
            let mut args = text.split(" ");
            match (args.next()) {
                Some("CALL")  => {
                    let ip = args.next().ok_or("Malformed Packet: Missing IP")?.to_string();
                    let str_port = args.next().ok_or("Malformed Packet: Missing port")?;
                    let port = str_port.parse::<u16>().map_err(|_| "Invalid port number")?;
                    let mimetype = args.next().ok_or("Malformed Packet: Missing Mimetype")?.to_string();
                    let username = args.collect::<Vec<&str>>().join(" ").replace('"', "");
                    if username.is_empty() {
                        return Err("Malformed Packet: Missing Username")
                    } 
                    return Ok(VcpMessage::Call { ip: ip.to_string(), port, mimetype, username });
                }
                Some("ACCEPTCALL")  => {
                    let ip = args.next().ok_or("Malformed Packet: Missing IP")?.to_string();
                    let str_port = args.next().ok_or("Malformed Packet: Missing port")?;
                    let port = str_port.parse::<u16>().map_err(|_| "Invalid port number")?;
                    let mimetype = args.next().ok_or("Malformed Packet: Missing Mimetype")?.to_string();
                    let username = args.collect::<Vec<&str>>().join(" ").replace('"', "");
                    if username.is_empty() {
                        return Err("Malformed Packet: Missing Username")
                    } 
                    return Ok(VcpMessage::AcceptCall { ip, port, mimetype, username });
                }
                Some("DECLINECALL")  => {
                    let ip = args.next().ok_or("Malformed Packet: Missing IP")?.to_string();
                    let str_port = args.next().ok_or("Malformed Packet: Missing port")?;
                    let port = str_port.parse::<u16>().map_err(|_| "Invalid port number")?;
                    let mimetype = args.next().ok_or("Malformed Packet: Missing Mimetype")?.to_string();
                    let username = args.collect::<Vec<&str>>().join(" ").replace('"', "");

                    if username.is_empty() {
                        return Err("Malformed Packet: Missing Username")
                    } 
                    return Ok(VcpMessage::DeclineCall { ip, port, mimetype, username });
                }
                _ => Err("Unknown Command or Malformed Packet"),
            }

        }
    }
}

async fn udp_thread(
    sock: UdpSocket,
    udp_map_clone: Arc<Mutex<HashMap<String, Connection>>>
) {
    let mut buf = [0; 1500]; 
    println!("UDP task running...");

    let mut receivers: HashMap<String, VcpReceiver> = HashMap::new();

    let mut callpending: Option<SocketAddr> = None;

    loop {
        match sock.recv_from(&mut buf).await {
            Ok((amnt_read, addr)) => {
                let r = receivers.get_mut(&addr.to_string());
                match r {
                    None => {
                        let r = VcpReceiver::new(buf[0..amnt_read].to_vec());
                        receivers.insert(addr.to_string(), r);
                    }
                    Some(r) => {
                        r.feed(buf.to_vec());
                    }
                }

                let r = receivers.get_mut(&addr.to_string()).unwrap();
                if *r.get_state() == VcpReceptionState::Done {
                    match VcpMessage::parse(r.get_result()) {
                        Ok(res) => {
                            match res {
                                VcpMessage::Call { ip, port, mimetype, username } => {
                                    let text = format!("Incoming Call from {ip} port {port} with mimetype {mimetype} and username {username}");
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
                                }


                                VcpMessage::AcceptCall { ip, port, mimetype, username } => {
                                    let mut cmap = udp_map_clone.lock().unwrap();
                                    cmap.insert(callpending.unwrap().ip().to_string(), Connection::new());
                                    callpending = None;
                                },

                                VcpMessage::DeclineCall { ip, port, mimetype, username } => todo!(),


                                VcpMessage::Packet { packet_nr, timestamp, data_len, data } => {
                                    let mut cmap = udp_map_clone.lock().unwrap();

                                    if let Some(conn) = cmap.get_mut(&addr.ip().to_string()) {
                                        //ignore packet if old
                                        if packet_nr >= conn.jitter_buffer.last_popped {
                                            conn.jitter_buffer.add_packet(packet_nr, &data);
                                            let cur_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("System time broke").as_millis() as i64;
                                            let latency = cur_time - timestamp;
                                            conn.latency = latency as i16;
                                        }


                                    } else {
                                        //placeholder for later testing purposes
                                        panic!("Sending packets before connection intialized");
                                    }
                                },
                            }
                        },
                        Err(err) => eprintln!("Could not parse data: {}", err)
                    }
                }
            }
            Err(err) => eprintln!("Socket Receive Error: {}", err)
        };
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:7000").await?;
    eprintln!("Listening...");
    let connections_map = Arc::new(Mutex::new(HashMap::<String, Connection>::new()));

    let udp_handle = tokio::spawn(udp_thread(sock, Arc::clone(&connections_map)));

    //for taking snapshots of connections
    let cmap_clone = Arc::clone(&connections_map);
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            timer.tick().await;
            let mut cmap = cmap_clone.lock().unwrap();
            for (_, conn) in cmap.iter_mut() {
                conn.jitter_buffer.take_snapshot();
                let packet_loss = conn.jitter_buffer.calculate_packet_loss();
                conn.packet_loss = packet_loss;
            }
        }
    });

    let cmap_clone = Arc::clone(&connections_map);
    tokio::spawn(async move {
        unsafe { std::env::set_var("VCD_SOCKET", "127.0.0.1") };
        unsafe { std::env::set_var("VCD_SOCKET_PORT", "8432") };
        let sock = TcpSocket::new_v4().unwrap();
        let addr = "127.0.0.1:8432".parse().unwrap();
        sock.bind(addr)?;
        let tcp = sock.listen(1)?;
        match tcp.accept().await {
            Ok((mut conn, addr)) => {
                let mut buf = [0; 1500];
                let mut receiver = VcpReceiver::new(vec![]);
                let mut has_responded_to_call = false;

                //start with a dummy address
                let mut to_send_to: SocketAddr = SocketAddr::from_str("0.0.0.0:10000").unwrap();
                loop {
                    if let Ok(amt) = conn.read(&mut buf).await {
                        if receiver.get_state() != &VcpReceptionState::Done {
                            receiver.feed(buf.to_vec());
                        } else if !has_responded_to_call{
                            has_responded_to_call = true;

                            match VcpMessage::parse(receiver.get_result()) {
                                Ok(r) =>  {
                                    match r {
                                        VcpMessage::AcceptCall { ip, port, mimetype, username } => {
                                            let sock = UdpSocket::bind("0.0.0.0:0").await?;
                                            let response = format!("ACCEPTCALL {} {} {} \"{}\"\r\n", ip, port, mimetype, username);

                                            match SocketAddr::from_str(&format!("{}:{}", ip, port).to_string()) {
                                                Ok(addr) => {
                                                    to_send_to = addr;
                                                    if let Err(e) = sock.send_to(response.as_bytes(), to_send_to).await {
                                                        eprintln!("Failed to send resp {}", e);
                                                    }
                                                },
                                                Err(e) => {
                                                    eprintln!("Failed to convert ip {}", e);
                                                }
                                            };
                                            eprintln!("{} picked up {}:{}'s call with {}", username, ip, port, mimetype)
                                        }
                                        VcpMessage::DeclineCall { ip, port, mimetype, username } => {
                                            eprintln!("{} declined to pick up {}:{}'s call", username, ip, port)
                                        }
                                        _ => todo!()
                                    }
                                } 

                                Err(e) => {
                                    //this should probably be the same as declining the call
                                    eprintln!("Failed to parse msg: {}", e)
                                }
                            }
                        } else {

                            let keys: Vec<String> = {
                                let guard = cmap_clone.lock().unwrap();
                                guard.keys().cloned().collect()
                            };

                            for connection in keys {
                                let sock = UdpSocket::bind("0.0.0.0:0").await?;
                                sock.send_to(&buf, format!("{connection}:7000")).await?;
                            }
                        }
                        println!("read {} bytes from client", amt)
                    }
                }
            }
            Err(e) => println!("{}", e)
        }
        Ok::<(), std::io::Error>(())
    });

    std::future::pending::<()>().await;
    Ok(())
}
