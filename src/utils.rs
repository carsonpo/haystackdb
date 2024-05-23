pub mod quantization;

pub use quantization::{dequantize, quantize};

use brotli::CompressorWriter;
use brotli::Decompressor;
use std::io::prelude::*;

pub fn compress_string(input: &str) -> Vec<u8> {
    let mut compressed = Vec::new();
    {
        let mut compressor = CompressorWriter::new(&mut compressed, 4096, 11, 22);
        compressor
            .write_all(input.as_bytes())
            .expect("Failed to write data");
    }
    compressed
}

pub fn decompress_string(input: &[u8]) -> String {
    let mut decompressed = Vec::new();
    {
        let mut decompressor = Decompressor::new(input, 4096);
        decompressor
            .read_to_end(&mut decompressed)
            .expect("Failed to read data");
    }
    String::from_utf8(decompressed).expect("Failed to convert to string")
}
