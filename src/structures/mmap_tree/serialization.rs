pub trait TreeSerialization {
    fn serialize(&self) -> Vec<u8>;
}

pub trait TreeDeserialization {
    fn deserialize(data: &[u8]) -> Self
    where
        Self: Sized;
}

impl TreeDeserialization for i32 {
    fn deserialize(data: &[u8]) -> Self {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&data[..4]);
        i32::from_le_bytes(bytes)
    }
}

impl TreeSerialization for u128 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl TreeDeserialization for u128 {
    fn deserialize(data: &[u8]) -> Self {
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&data[..16]);
        u128::from_le_bytes(bytes)
    }
}

impl TreeSerialization for Vec<u8> {
    fn serialize(&self) -> Vec<u8> {
        self.clone()
    }
}

impl TreeDeserialization for Vec<u8> {
    fn deserialize(data: &[u8]) -> Self {
        data.to_vec()
    }
}

impl TreeSerialization for i32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl TreeDeserialization for String {
    fn deserialize(data: &[u8]) -> Self {
        if data.len() < 4 {
            panic!("Data too short to contain length prefix");
        }
        let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize; // Read length
        if data.len() < 4 + len {
            panic!("Data too short for specified string length");
        }
        let string_data = &data[4..4 + len]; // Extract string data
        String::from_utf8(string_data.to_vec()).unwrap()
    }
}

impl TreeSerialization for String {
    fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(self.len() as u32).to_le_bytes()); // Write length
        data.extend_from_slice(self.as_bytes()); // Write string data
        data
    }
}
