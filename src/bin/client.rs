use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::Context;
use arrayvec::ArrayVec;
use bytes::Bytes;
use clap::Parser;
use h2::client::{self, SendRequest};
use http::Request;
use http2_multiplexing::{Message, MessageType};
use thiserror::Error;
use tokio::{net::TcpStream, task::JoinSet};

#[derive(Parser)]
#[command(version, next_line_help = true)]
struct Cli {
    /// Адрес сервера, к котрому подключаться.
    #[arg(short, long, value_name = "ADDRESS", default_value = "127.0.0.1:8080")]
    server_address: SocketAddr,

    /// Время ожидаения установки соединения.
    #[arg(long, value_name = "SECONDS", default_value = "2")]
    #[arg(value_parser = |arg: &str| arg.parse().map(Duration::from_secs))]
    connect_timeout: Duration,

    /// Количество потоков, которые будут выделены для отправки запросов.
    #[arg(long, value_name = "NUMBER", default_value_t = 1)]
    workers: usize,

    /// Количество сообщений для отправки.
    ///
    /// Допустимый диапозон от 1 до 100.
    #[arg(short, long, value_name = "NUMBER", default_value_t = 50)]
    #[arg(value_parser = clap::value_parser!(u8).range(1..=100))]
    count: u8,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Конфигурация асинхронного рантайма
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.workers)
        .thread_name("client-worker")
        .enable_all()
        .build()?;

    // Запуск логгера
    tracing_subscriber::fmt::init();

    runtime.block_on(main_task(cli))?;

    Ok(())
}

async fn main_task(cli: Cli) -> anyhow::Result<()> {
    let tcp = TcpStream::connect(cli.server_address)
        .await
        .context("Не удалось подключится к серверу")?;

    let start_time = Instant::now();

    let (client, connection) = client::handshake(tcp).await.context("HTTP/2 handshake")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::error!("Соединение разорвано: {}", err);
        }
    });

    let mut request_time = Vec::new();

    // 3.4. HTTP/2 Connection Preface
    //
    // To avoid unnecessary latency, clients are permitted to send additional
    // frames to the server immediately after sending the client connection
    // preface, without waiting to receive the server connection preface.
    //
    // Текущий API h2 не позволяет ждать подтверждения соединения от сервера.
    // Ждём ответ на первый запрос.
    match tokio::time::timeout(cli.connect_timeout, send_request(0, client.clone())).await {
        Ok(Ok(duration)) => request_time.push(duration),
        Ok(Err(err)) => tracing::error!("Ошибка при отправке запроса: {:?}", err),
        Err(_) => {
            tracing::info!("Время установки соединения истекло");
            return Ok(());
        }
    }

    let mut request_set: JoinSet<Result<Duration, Error>> = JoinSet::new();
    for id in 1..cli.count as usize {
        request_set.spawn(send_request(id, client.clone()));
    }

    while let Some(result) = request_set.join_next().await {
        match result {
            Ok(Ok(duration)) => request_time.push(duration),
            Ok(Err(err)) => tracing::error!("Ошибка при отправке запроса: {:?}", err),
            Err(err) => tracing::error!("Паника при отправке запроса: {:?}", err),
        }
    }

    let total_time = Instant::now().duration_since(start_time);
    let request_count = request_time.len();

    let (min, max, sum) = if request_count > 0 {
        request_time.into_iter().fold(
            (Duration::MAX, Duration::ZERO, Duration::ZERO),
            |(min, max, sum), duration| (min.min(duration), max.max(duration), sum + duration),
        )
    } else {
        (Duration::ZERO, Duration::ZERO, Duration::ZERO)
    };
    let avg = sum.checked_div(request_count as u32).unwrap_or_default();

    tracing::info!("Соединение с сервером {} завершено: количество запросов - {}, время ожидания ответа - [min = {:?}, max = {:?}, avg = {:?}], общее время - {:?}",
                   cli.server_address,
                   request_count,
                   min,
                   max,
                   avg,
                   total_time
                );

    Ok(())
}

#[derive(Debug, Error)]
enum Error {
    #[error("Ошибка HTTP2")]
    H2(#[from] h2::Error),
    #[error("Ошибка (де)сереализации")]
    Bincode(#[from] bincode::Error),
    #[error("Буфер слишком маленький")]
    BufferSize(usize),
    #[error("Неверный тип сообщения")]
    BadMessage(usize),
}

async fn send_request(id: usize, client: SendRequest<Bytes>) -> Result<Duration, Error> {
    let mut buf = ArrayVec::<u8, 16>::new();
    let message = Message {
        id,
        ty: MessageType::Ping,
    };

    let request = Request::get("http://localhost.local").body(()).unwrap();
    let (response, mut stream) = client.ready().await?.send_request(request, false)?;
    stream.send_data(
        Bytes::from(bincode::serialize(&message).expect("Ошибка сериализации")),
        true,
    )?;

    let start_time = Instant::now();
    let response = response.await?;
    let mut body = response.into_body();

    while let Some(data) = body.data().await {
        let data = data?;
        let len = data.len();
        if buf.len() + len <= buf.capacity() {
            buf.extend(data);
        } else {
            return Err(Error::BufferSize(id));
        }
        let _ = body.flow_control().release_capacity(len);
    }

    let message: Message = bincode::deserialize(&buf)?;
    if message.id != id || message.ty != MessageType::Pong {
        return Err(Error::BadMessage(id));
    }

    Ok(Instant::now().duration_since(start_time))
}
