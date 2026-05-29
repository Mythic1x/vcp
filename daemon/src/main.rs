mod vcpreciever;

use std::cmp;
use std::net::SocketAddr;
use std::time::Duration;
use std::{io, str::FromStr};
use tokio::net::UdpSocket;
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
        if self.snapshots.len() < 10 {return 0.0};
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
    fn parse(bytes: &[u8]) -> Result<Self, String> {
        if bytes.starts_with(b"PACKET/")  {
           let packet_nr_bytes = &bytes[7..15];
           let packet_nr = u64::from_be_bytes(packet_nr_bytes.try_into().map_err(|_| "Malformed packet found when parsing packet number".to_string())?);
           let data_len_bytes = &bytes[15..17]; 
           let data_len = u16::from_be_bytes(data_len_bytes.try_into().map_err(|_| "Malformed packet found when parsing data length".to_string())?);
           let data_len_us = data_len as usize;
           if bytes.len() < 17 + data_len_us {
               return Err("Malformed packet: data length exceeds packet size".to_string());
           }
           let data = bytes[17..17 + data_len_us].to_vec();
           return Ok(VcpMessage::Packet { packet_nr, data_len, data });
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
                        return Err("Malformed Packet: Missing Username".to_string())
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
                        return Err("Malformed Packet: Missing Username".to_string())
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
                        return Err("Malformed Packet: Missing Username".to_string())
                    } 
                    return Ok(VcpMessage::DeclineCall { ip, port, mimetype, username });
                }
                _ => Err("Unknown Command or Malformed Packet".to_string()),
            }

        }
    }
}


#[tokio::main]
async fn main() -> io::Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:7000").await?;
    println!("Listening...");
    let connections_map = Arc::new(Mutex::new(HashMap::<core::net::SocketAddr, JitterBuffer>::new()));

    let udp_map_clone = Arc::clone(&connections_map);
    let udp_handle = tokio::spawn(async move {
    let mut buf = [0; 1500]; 
    println!("UDP task running...");

    //may use later for tcp but not rn
    //let mut receivers: HashMap<String, VcpReceiver> = HashMap::new();
    loop {
        match sock.recv_from(&mut buf).await {
              Ok((data, addr)) => {
                match VcpMessage::parse(&buf[..data]) {
                    Ok(res) => {
                        match res {


                            VcpMessage::Call { ip, port, mimetype, username } => {
                                let text = format!("Incoming Call from {ip} port {port} with mimetype {mimetype} and username {username}");
                                println!("{text}");
                                
                                if let Err(e) = sock.send_to(text.as_bytes(), addr).await {
                                    println!("Failed to send Call response: {}", e);
                                }
                            }


                            VcpMessage::AcceptCall { ip, port, mimetype, username } => {
                                let mut cmap = udp_map_clone.lock().unwrap();
                                cmap.insert(addr, JitterBuffer::new());
                            },

                            VcpMessage::DeclineCall { ip, port, mimetype, username } => todo!(),


                            VcpMessage::Packet { packet_nr, data_len, data } => {
                                let mut cmap = udp_map_clone.lock().unwrap();

                                if let Some(jitter_buffer) = cmap.get_mut(&addr) {
                                    //ignore packet if old
                                    if packet_nr >= jitter_buffer.last_popped {
                                        jitter_buffer.add_packet(packet_nr, &data);
                                    }

                                } else {
                                    //placeholder for later testing purposes
                                    panic!("Sending packets before connection intialized");
                                }
                            },
                        }
                    },
                    Err(err) => println!("Parse Error: {}", err),
                }
              },
              Err(err) => println!("Socket Receive Error: {}", err),
        }
    }
});

   //for taking snapshots of connections
    let cmap_clone = Arc::clone(&connections_map);
    tokio::spawn(async move {
       let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
        timer.tick().await;
        let mut cmap = cmap_clone.lock().unwrap();
            for (_, jbuffer) in cmap.iter_mut() {
                jbuffer.take_snapshot();
            }
        }
   });

    std::future::pending::<()>().await;
    Ok(())
    
}

/*  Ok((amnt_read, addr)) => {
                let r = receivers.get_mut(&addr.to_string());
                match r {
                    None => {
                        let r = VcpReceiver::new(buf[0..amnt_read].to_vec());
                        if *r.get_state() == VcpReceptionState::Done {
                            println!("{:?}", String::from_utf8(r.get_result().clone()));
                        }
                        receivers.insert(addr.to_string(), r);
                    }
                    Some(r) => {
                        r.feed(buf.to_vec());
                        if *r.get_state() == VcpReceptionState::Done {
                            println!("{:?}", String::from_utf8(r.get_result().clone()));
                        }
                        println!("{:?}", r.get_state()); */