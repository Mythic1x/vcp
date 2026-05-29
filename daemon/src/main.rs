mod vcpreciever;

use std::net::SocketAddr;
use std::{io, str::FromStr};
use tokio::net::UdpSocket;
use std::collections::HashMap;
use std::collections::BTreeMap;
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
}



impl JitterBuffer {
    fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
            last_popped: 0,
            highest_packet_nr: 0,
        }
    }

    fn calculate_packet_loss(&self) -> f64 {
        let Some((last_key, _)) = self.buffer.last_key_value() else { return 0.0 };
        let Some((first_key, _)) = self.buffer.first_key_value() else {return 0.0;};
        let expected_packets = last_key - first_key + 1;
        let received_packets = self.buffer.len() as u64;
        let diff = expected_packets - received_packets;
        let packet_loss_ratio = (diff as f64) / (expected_packets as f64);
        return packet_loss_ratio * 100.0;
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
            match (args.next(),args.next()) {
                (Some("CALL"), Some(ip)) => {
                    let str_port = args.next().ok_or("Malformed Packet: Missing port")?;
                    let port = str_port.parse::<u16>().map_err(|_| "Invalid port number")?;
                    let mimetype = args.next().ok_or("Malformed Packet: Missing Mimetype")?.to_string();
                    let username = args.collect::<Vec<&str>>().join(" ").replace('"', "");
                    if username.is_empty() {
                        return Err("Malformed Packet: Missing Username".to_string())
                    } 
                    return Ok(VcpMessage::Call { ip: ip.to_string(), port, mimetype, username });
                }
                (Some("ACCEPT"), Some("CALL")) => {
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
                (Some("DECLINE"), Some("CALL")) => {
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

    let mut receivers: HashMap<String, VcpReceiver> = HashMap::new();
    loop {
        match sock.recv_from(&mut buf).await {
            Ok((amnt_read, addr)) => {
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
                        println!("{:?}", r.get_state());
                    }
                }
            }
            Err(err) => println!("Socket Receive Error: {}", err),
        }
    }
});
   udp_handle.await.expect("UDP task failed");
   Ok(())
    
}
