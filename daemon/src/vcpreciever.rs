#[derive(PartialEq, std::fmt::Debug, Eq, Clone, Copy)]
pub enum VcpReceptionState {
    Action,
    UnQuotedArg,
    QuotedArg,
    PacketNr,
    PacketLen,
    PacketData,
    PacketTS,
    Done,
}

pub enum CallState {
    Accept,
    Decline,
    Request
}

pub struct VcpReceiver {
    final_action: Vec<u8>,
    backlog: Vec<u8>,
    currently_parsing: Vec<u8>,
    parsing_pos: usize,
    state: VcpReceptionState,
    packetlen: u16,
    packet_data: Vec<u8>,
    packet_ts: u64,
    packet_nr: u64,
    action_name: String,
    args: Vec<String>
}


/* parses the following argument grammar: <action>(" "<arg>)*("/"<u64><u64><u64><u64><u8>*) */
impl VcpReceiver {
    pub fn new(bytes: Vec<u8>) -> VcpReceiver {
        let mut r = VcpReceiver {
            parsing_pos: 0,
            backlog: bytes,
            currently_parsing: vec![],
            state: VcpReceptionState::Action,
            final_action: vec![],
            packetlen: 0,
            packet_data: vec![],
            action_name: "".to_owned(),
            packet_ts: 0,
            packet_nr: 0,
            args: vec![]
        };

        while r.has_bytes() && r.state != VcpReceptionState::Done{
            r.consume();
        }

        return r
    }

    fn has_bytes(&self) -> bool {
        return self.parsing_pos < self.backlog.len()
    }

    fn cur_byte(&self) -> u8 {
        return self.backlog[self.parsing_pos]
    }

    pub fn consume(&mut self) {
        let b = self.cur_byte();

        type A = VcpReceptionState;
        self.state = match self.state {
            A::Action => {
                if b == ' ' as u8 {
                    if let Ok(name) = String::from_utf8(self.currently_parsing.clone()) {
                        self.final_action.extend(&self.currently_parsing);
                        self.final_action.push(0x20);
                        self.action_name = name;
                        self.currently_parsing.clear();
                        A::UnQuotedArg
                    } else {
                        eprintln!("Got invalid action: {:?}", self.currently_parsing);
                        self.currently_parsing.clear();
                        A::Action
                    }
                } else if b == '/' as u8  {
                    self.final_action.extend(&self.currently_parsing);
                    self.final_action.push('/' as u8);
                    self.action_name = String::from("PACKET");
                    self.currently_parsing.clear();
                    A::PacketNr
                } else if b == '\n' as u8 {
                    A::Action
                } else {
                    self.currently_parsing.push(b);
                    A::Action
                }
            }

            A::PacketNr => {
                self.currently_parsing.push(b);
                if self.currently_parsing.len() == 8 {
                    self.final_action.extend(&self.currently_parsing);
                    self.packet_nr = u64::from_be_bytes(self.currently_parsing.clone().try_into().expect("could not convert bytes into 8 wide word for packet TS"));
                    self.currently_parsing.clear();
                    A::PacketTS
                } else {
                    A::PacketNr
                }
            }

            A::PacketLen => {
                self.currently_parsing.push(b);
                if self.currently_parsing.len() == 2 {
                    self.final_action.extend(&self.currently_parsing);
                    self.packetlen = u16::from_be_bytes(self.currently_parsing.clone().try_into().expect("could not convert bytes into 8 wide word for packet data length"));
                    self.currently_parsing.clear();
                    if self.packetlen == 0 {
                        self.packet_data = vec![];
                        A::Done
                    } else {
                        A::PacketData
                    }
                } else {
                    A::PacketLen
                }
            }

            A::PacketTS => {
                self.currently_parsing.push(b);
                if self.currently_parsing.len() == 8 {
                    self.final_action.extend(&self.currently_parsing);
                    self.packet_ts = u64::from_be_bytes(self.currently_parsing.clone().try_into().expect("could not convert bytes into 8 wide word for packet TS"));
                    self.currently_parsing.clear();
                    A::PacketLen
                } else {
                    A::PacketTS
                }
            }

            A::PacketData => {
                self.currently_parsing.push(b);
                if self.currently_parsing.len() == self.packetlen as usize{
                    self.packet_data.extend(&self.currently_parsing);
                    self.currently_parsing.clear();
                    self.final_action.extend(&self.packet_data);
                    A::Done
                } else {
                    A::PacketData
                }
            }

            A::UnQuotedArg => {
                if b == ' ' as u8 {
                    self.final_action.extend(&self.currently_parsing);
                    self.final_action.push(0x20);
                    self.args.push(String::from_utf8_lossy(&self.currently_parsing).to_string());
                    self.currently_parsing.clear();
                    A::UnQuotedArg
                } else if b == '"' as u8 {
                    self.final_action.extend(&self.currently_parsing);
                    if self.currently_parsing.len() > 0 {
                        self.args.push(String::from_utf8_lossy(&self.currently_parsing).to_string());
                    }
                    self.currently_parsing.clear();

                    self.currently_parsing.push(b);
                    A::QuotedArg
                } else if b == '\r' as u8 || b == '\n' as u8 {
                    self.final_action.extend(&self.currently_parsing);
                    if self.currently_parsing.len() > 0 {
                        self.args.push(String::from_utf8_lossy(&self.currently_parsing).to_string());
                    }
                    self.currently_parsing.clear();
                    A::Done
                } else {
                    self.currently_parsing.push(b);
                    A::UnQuotedArg
                }
            }

            A::QuotedArg => {
                if b == '"' as u8 {
                    self.currently_parsing.push(b);
                    self.final_action.extend(&self.currently_parsing);
                    self.args.push(String::from_utf8_lossy(&self.currently_parsing).to_string());
                    self.currently_parsing.clear();
                    A::UnQuotedArg
                } else {
                    self.currently_parsing.push(b);
                    A::QuotedArg
                }
            }

            A::Done => {
                A::Done
            }
        };

        self.parsing_pos += 1;
    }

    ///Gets the current packet data, and clears it for the next call of this function
    ///can be called when feed returns the PacketData variant
    pub fn stream_packet_data(&mut self) -> Vec<u8> {
        let data = self.packet_data.clone();
        self.packet_data.clear();
        return data;
    }

    ///Resets all state for a new action
    pub fn reset(&mut self) {
        if self.parsing_pos < self.backlog.len() {
            self.backlog = self.backlog[self.parsing_pos..].to_vec();
        } else {
            self.backlog.clear();
        }
        self.parsing_pos = 0;
        self.currently_parsing.clear();
        self.packetlen = 0;
        self.packet_ts = 0;
        self.packet_nr = 0;
        self.state = VcpReceptionState::Action;
        self.final_action.clear();
        self.packet_data.clear();
    }

    pub fn get_action(&self) -> &String {
        return &self.action_name;
    }

    ///Feed new bytes into the machine
    ///returns the current state
    pub fn feed(&mut self, bytes: Vec<u8>) -> &VcpReceptionState {
        self.backlog.extend(bytes);

        while self.has_bytes() && self.state != VcpReceptionState::Done {
            self.consume();
        }

        return &self.state
    }

    pub fn get_state(&self) -> &VcpReceptionState {
        return &self.state
    }

    ///Returns the final result, should be called when feed returns the Done variant
    pub fn get_result(&self) -> &Vec<u8> {
        return &self.final_action
    }

    pub fn get_args(&self) -> &Vec<String> {
        return &self.args;
    }

    pub fn get_packet_nr(&self) -> u64 {
        return self.packet_nr;
    }

    pub fn get_packet_data(&self) -> &Vec<u8> {
        return &self.packet_data;
    }

    pub fn get_packet_len(&self) -> u16 {
        return self.packetlen;
    }

    pub fn get_packet_ts(&self) -> u64 {
        return self.packet_ts;
    }
}
