use std::io;

use rmpv::{Value, decode::read_value, encode::write_value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

pub struct FramedMsgpackTransport {
    stream: UnixStream,
}

impl FramedMsgpackTransport {
    pub async fn connect(path: &std::path::Path) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self { stream })
    }

    pub async fn read_value(&mut self) -> io::Result<Value> {
        loop {
            let frame_len = self.read_frame_len().await?;
            if frame_len == 0 {
                // Some daemon messages can contain empty keepalive frames.
                // Skip them and keep waiting for the next payload frame.
                continue;
            }

            let mut payload = vec![0u8; frame_len as usize];
            self.stream.read_exact(&mut payload).await?;
            let mut cursor = std::io::Cursor::new(payload);
            return read_value(&mut cursor).map_err(io::Error::other);
        }
    }

    pub async fn write_value(&mut self, value: &Value) -> io::Result<()> {
        let mut payload = Vec::new();
        write_value(&mut payload, value).map_err(io::Error::other)?;

        let mut len_prefix = Vec::new();
        write_value(&mut len_prefix, &Value::from(payload.len() as u32))
            .map_err(io::Error::other)?;

        self.stream.write_all(&len_prefix).await?;
        self.stream.write_all(&payload).await?;
        self.stream.flush().await
    }

    async fn read_frame_len(&mut self) -> io::Result<u32> {
        let marker = self.stream.read_u8().await?;
        let value = match marker {
            0x00..=0x7f => marker as i64,
            0xe0..=0xff => (marker as i8) as i64,
            0xcc => self.stream.read_u8().await? as i64,
            0xcd => self.stream.read_u16().await? as i64,
            0xce => self.stream.read_u32().await? as i64,
            0xd0 => self.stream.read_i8().await? as i64,
            0xd1 => self.stream.read_i16().await? as i64,
            0xd2 => self.stream.read_i32().await? as i64,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported msgpack integer marker for frame length: 0x{marker:02x}"),
                ));
            }
        };

        if value < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative frame length",
            ));
        }

        Ok(value as u32)
    }
}
