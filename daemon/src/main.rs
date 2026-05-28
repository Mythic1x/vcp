mod vcpreciever;

use std::{io, str::FromStr};
use tokio::net::UdpSocket;

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

impl VcpMessage {
    fn parse(bytes: &[u8]) -> Result<Self, String> {
        if bytes.starts_with(b"PACKET/")  {
           let packet_nr_bytes = &bytes[7..15];
           let packet_nr = u64::from_be_bytes(packet_nr_bytes.try_into().map_err(|_| "Malformed packet found when parsing packet number".to_string())?);
           let data_len_bytes = &bytes[15..17]; 
           let data_len = u16::from_be_bytes(data_len_bytes.try_into().map_err(|_| "Malformed packet found when parsing data length".to_string())?);
           let data = bytes[17..].to_vec();
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
    let mut buf = [0; 1500];
    println!("Listening...");

    loop {
        match sock.recv_from(&mut buf).await {
            Ok((data, addr)) => {
                match VcpMessage::parse(&buf[..data]) {
                    Ok(res) => {
                      match res {
                        VcpMessage::Call { ip, port, mimetype, username } => {
                            let text = format!("Incoming Call from {ip} port {port} with mimetype {mimetype} and username {username}");
                            println!("{text}");
                            let bytes: &[u8] = text.as_bytes();
                            sock.send_to(bytes, addr).await?;
                        }
                        VcpMessage::AcceptCall { ip, port, mimetype, username } => todo!(),
                        VcpMessage::DeclineCall { ip, port, mimetype, username } => todo!(),
                        VcpMessage::Packet { packet_nr, data_len, data } => todo!(),
                        }  
                    }
                    Err(err) => println!("{err}"),
                }
                sock.send_to(&buf[..data], addr).await?;
            }
            Err(err) => println!("{}", err),
        }
    }
}
