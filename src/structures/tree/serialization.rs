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

impl TreeSerialization for i32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl TreeDeserialization for String {
    fn deserialize(data: &[u8]) -> Self {
        let mut bytes = Vec::new();
        let mut i = 4;
        while i < data.len() {
            let len = data[i..i + 4].try_into().unwrap();
            let len = i32::from_le_bytes(len) as usize;
            let start = i + 4;
            let end = start + len;
            bytes.extend_from_slice(&data[start..end]);
            i = end;
        }
        String::from_utf8(bytes).unwrap()
    }
}

impl TreeSerialization for String {
    fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(self.len() as i32).to_le_bytes());
        data.extend_from_slice(self.as_bytes());
        data
    }
}
