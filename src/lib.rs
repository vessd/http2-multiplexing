use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    Ping,
    Pong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub id: usize,
    pub ty: MessageType,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Ошибка HTTP2")]
    H2(#[from] h2::Error),
    #[error("Ошибка (де)сереализации")]
    Bincode(#[from] bincode::Error),
    #[error("Буфер слишком маленький")]
    BufferSize(Option<usize>),
    #[error("Неверный тип сообщения")]
    BadMessage(usize),
    #[error("Клиент не дождался ответа")]
    ClientTimeout,
}
