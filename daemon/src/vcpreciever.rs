#[derive(PartialEq)]
pub enum VcpReceptionState {
    Action,
    Ip,
    Port,
    Mimetype,
    UserNameBegin,
    UserNameMiddle,
    PacketNr,
    PacketLen,
    PacketData,
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
    packetlen: u64,
    packet_data: Vec<u8>
}

impl VcpReceiver {
    pub fn new(bytes: Vec<u8>) -> VcpReceiver {
        let mut r = VcpReceiver {
            parsing_pos: 0,
            backlog: bytes,
            currently_parsing: vec![],
            state: VcpReceptionState::Action,
            final_action: vec![],
            packetlen: 0,
            packet_data: vec![]
        };

        while r.has_bytes() {
            r.consume();
        }

        return r
    }

    fn has_bytes(&self) -> bool {
        return self.parsing_pos < self.currently_parsing.len()
    }

    fn cur_byte(&self) -> u8 {
        return self.currently_parsing[self.parsing_pos]
    }

    pub fn consume(&mut self) {
        let b = self.cur_byte();

        type A = VcpReceptionState;
        self.state = match self.state {
            A::Action => {
                if b == 0x20 /* SPACE */ {
                    self.final_action.extend(&self.currently_parsing);
                    self.final_action.push(0x20);
                    self.currently_parsing.clear();
                    A::Ip
                } else if b == 0x2F /* / */  {
                    self.final_action.extend(&self.currently_parsing);
                    self.final_action.push(0x2F);
                    self.currently_parsing.clear();
                    A::PacketNr
                } else {
                    self.currently_parsing.push(b);
                    A::Action
                }
            }

            A::PacketNr => {
                self.currently_parsing.push(b);
                if self.currently_parsing.len() == 7 {
                    self.final_action.extend(&self.currently_parsing);
                    self.currently_parsing.clear();
                    A::PacketLen
                } else {
                    A::PacketNr
                }
            }

            A::PacketLen => {
                self.currently_parsing.push(b);
                if self.currently_parsing.len() == 7 {
                    self.final_action.extend(&self.currently_parsing);
                    self.packetlen = u64::from_be_bytes(self.currently_parsing.clone().try_into().expect("could not convert bytes into 8 wide word for packet data length"));
                    A::PacketData
                } else {
                    A::PacketLen
                }
            }

            A::PacketData => {
                self.currently_parsing.push(b);
                self.packet_data.push(b);
                if self.currently_parsing.len() == self.packetlen.try_into().unwrap() {
                    A::Done
                } else {
                    A::PacketData
                }
            }

            A::Ip => {
                if b == 0x20 {
                    self.final_action.extend(self.currently_parsing.clone());
                    self.final_action.push(0x20);
                    self.currently_parsing.clear();
                    A::Port
                } else {
                    self.currently_parsing.push(b);
                    A::Ip
                }
            }

            A::Port => {
                if b == 0x20 {
                    self.final_action.extend(self.currently_parsing.clone());
                    self.final_action.push(0x20);
                    self.currently_parsing.clear();
                    A::Mimetype
                } else {
                    self.currently_parsing.push(b);
                    A::Port
                }
            }
            A::Mimetype => {
                if b == 0x20 {
                    self.final_action.extend(self.currently_parsing.clone());
                    self.final_action.push(0x20);
                    self.currently_parsing.clear();
                    A::UserNameBegin
                } else {
                    self.currently_parsing.push(b);
                    A::Mimetype
                }
            }
            A::UserNameBegin => {
                if b == 0x22 /* " */ {
                    self.currently_parsing.push(b);
                    A::UserNameMiddle
                } else {
                    A::UserNameBegin
                }
            },
            A::UserNameMiddle => {
                self.currently_parsing.push(b);
                if b == 0x22 /* " */ {
                    self.final_action.extend(self.currently_parsing.clone());
                    self.final_action.push(0x22);
                    A::Done
                } else {
                    A::UserNameMiddle
                }
            },
            A::Done => {
                self.backlog.push(b);
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
        self.parsing_pos = 0;
        self.currently_parsing.clear();
        self.packetlen = 0;
        self.state = VcpReceptionState::Action;
        self.final_action.clear();
        self.packet_data.clear()
    }

    ///Feed new bytes into the machine
    ///returns the current state
    pub fn feed(&mut self, bytes: Vec<u8>) -> &VcpReceptionState {
        self.backlog.extend(bytes);

        while self.has_bytes() {
            self.consume();
        }

        return &self.state
    }

    ///Returns the final result, should be called when feed returns the Done variant
    pub fn get_result(&self) -> &Vec<u8> {
        return &self.final_action
    }
}
