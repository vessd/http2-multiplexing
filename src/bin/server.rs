use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use arrayvec::ArrayVec;
use bytes::Bytes;
use clap::Parser;
use h2::{
    server::{self, SendResponse},
    RecvStream,
};
use http::Request;
use http2_multiplexing::{Message, MessageType};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, next_line_help = true)]
struct Cli {
    /// Адрес, на котором сервер ожидает подключения.
    #[arg(short, long, value_name = "ADDRESS", default_value = "0.0.0.0:8080")]
    listen_address: SocketAddr,

    /// Количество потоков, которые будут выделены для обработки.
    ///
    /// Если значение равно 0 или больше `std::thread::available_parallelism()`, то будет использовано значение `available_parallelism`.
    #[arg(long, value_name = "NUMBER", default_value_t = 0)]
    workers: usize,

    /// Максимальное количество одновременных подключений.
    #[arg(long, value_name = "NUMBER", default_value_t = 5)]
    connection_limit: usize,
}

impl Cli {
    fn worker_threads(&self) -> usize {
        let available_cores = std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::MIN)
            .get();
        match self.workers {
            0 => available_cores,
            n if n > available_cores => available_cores,
            n => n,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Конфигурация асинхронного рантайма
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.worker_threads())
        .thread_name("server-worker")
        .enable_all()
        .build()?;

    // Запуск логгера
    tracing_subscriber::fmt::init();

    tracing::info!(
        workers = cli.worker_threads(),
        connection_limit = cli.connection_limit,
        "Запуск сервера"
    );

    runtime.block_on(main_task(cli))?;

    Ok(())
}

struct ServerMetrics {
    client_time: Vec<Duration>,
    client_timeout: u64,
}

impl ServerMetrics {
    fn new() -> Self {
        Self {
            client_time: Vec::new(),
            client_timeout: 0,
        }
    }

    fn push(&mut self, client_result: Result<Result<Duration, h2::Error>, JoinError>) {
        match client_result {
            Ok(Ok(duration)) => self.client_time.push(duration),
            Ok(Err(err)) => tracing::error!("Ошибка при обработки соединения {}", err.to_string()),
            Err(err) => tracing::error!("Паника при обработки соединения {}", err.to_string()),
        }
    }

    fn finish(self) {
        let connection_count = self.client_time.len();
        let (min, max, sum) = if connection_count > 0 {
            self.client_time.into_iter().fold(
                (Duration::MAX, Duration::ZERO, Duration::ZERO),
                |(min, max, sum), duration| (min.min(duration), max.max(duration), sum + duration),
            )
        } else {
            (Duration::ZERO, Duration::ZERO, Duration::ZERO)
        };
        let avg = sum.checked_div(connection_count as u32).unwrap_or_default();

        tracing::info!("Статистика: количество подключений - {}, время обработки - [min = {:?}, max = {:?}, avg = {:?}], количество тайм-аутов - {}",
                       connection_count,
                       min,
                       max,
                       avg,
                       self.client_timeout
                    );
    }
}

async fn main_task(cli: Cli) -> anyhow::Result<()> {
    let listener = TcpListener::bind(cli.listen_address)
        .await
        .context(format!(
            "Не удалось привязать сокет к адресу {}",
            cli.listen_address,
        ))?;

    tracing::info!("Сервер начал слушать адрес {}", cli.listen_address);

    let mut mertrics = ServerMetrics::new();
    let mut connection_set: JoinSet<Result<Duration, h2::Error>> = JoinSet::new();
    let token = CancellationToken::new();
    let connection_control = Arc::new(Semaphore::new(cli.connection_limit));

    // Завершение работы по Ctrl-C
    {
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(err) = tokio::signal::ctrl_c().await {
                tracing::error!(
                    error = err.to_string(),
                    "Не удалось подписаться на сигнал SIGINT"
                );
            }
            tracing::info!("Получен сигнал SIGINT");
            token.cancel();
        });
    }

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                break;
            }
            Some(result) = connection_set.join_next() => {
                mertrics.push(result);
            }
            socket = listener.accept() => {
                match socket {
                    Ok((socket, addr)) => {
                        connection_set.spawn(serve(socket, addr, connection_control.clone(), token.clone()));
                    }
                    Err(err) => {
                        tracing::error!("Неудалось установить соединение {}", err);
                        continue;
                    }
                }
            }
        }
    }

    tracing::info!("Завершение работы");

    // Ждём закрытия текущих подключений
    while let Some(result) = connection_set.join_next().await {
        mertrics.push(result);
    }

    mertrics.finish();

    Ok(())
}

struct ClientMetrics {
    addr: SocketAddr,
    request_time: Vec<Duration>,
    // TODO: use quanta?
    start_time: Instant,
}

impl ClientMetrics {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            request_time: Vec::new(),
            start_time: Instant::now(),
        }
    }

    fn push(&mut self, request_result: Result<anyhow::Result<Duration>, JoinError>) {
        match request_result {
            Ok(Ok(duration)) => self.request_time.push(duration),
            Ok(Err(err)) => tracing::error!("Ошибка при обработки запроса {}", err.to_string()),
            Err(err) => tracing::error!("Паника при обработки запроса {}", err.to_string()),
        }
    }

    fn finish(self) -> Duration {
        let serve_time = Instant::now().duration_since(self.start_time);
        let request_count = self.request_time.len();
        let (min, max, sum) = if request_count > 0 {
            self.request_time.into_iter().fold(
                (Duration::MAX, Duration::ZERO, Duration::ZERO),
                |(min, max, sum), duration| (min.min(duration), max.max(duration), sum + duration),
            )
        } else {
            (Duration::ZERO, Duration::ZERO, Duration::ZERO)
        };
        let avg = sum.checked_div(request_count as u32).unwrap_or_default();

        tracing::info!("Соединение с клиентом {} завершено: количество запросов - {}, время обработки - [min = {:?}, max = {:?}, avg = {:?}], общее время - {:?}",
                       self.addr,
                       request_count,
                       min,
                       max,
                       avg,
                       serve_time
                    );

        serve_time
    }
}

async fn serve(
    socket: TcpStream,
    addr: SocketAddr,
    connection_control: Arc<Semaphore>,
    token: CancellationToken,
) -> Result<Duration, h2::Error> {
    let mut metrics = ClientMetrics::new(addr);
    let _permit = connection_control
        .acquire()
        .await
        .expect("connection_control");

    let mut connection = server::handshake(socket).await?;
    let mut request_set: JoinSet<anyhow::Result<Duration>> = JoinSet::new();

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                break;
            }
            Some(result) = request_set.join_next() => {
                metrics.push(result);
                tracing::trace!("request complete");
            }
            request = connection.accept() => {
                match request {
                    Some(Ok((request, respond))) => {
                        request_set.spawn(handle_request(request, respond));
                    }
                    Some(Err(err)) => {
                        if err.is_io() {
                            let err = err.into_io().unwrap();
                            if err.kind() == std::io::ErrorKind::NotConnected {
                                // клиент бросил соединение ?
                                // странный баг, не смог разобраться
                                // tracing::error!("Ошибка получения запроса: NotConnected");
                            } else {
                                tracing::error!("Ошибка получения запроса: {:?}", err);
                            }
                        } else {
                            tracing::error!("Ошибка получения запроса: {:?}", err);
                        }

                        break;
                    }
                    None => {
                        // Cоединение закрыто
                        break;
                    }
                }
            }
        };
    }

    // Ждём завершения обработки уже принятых запросов
    while let Some(result) = request_set.join_next().await {
        metrics.push(result);
    }

    Ok(metrics.finish())
}

async fn handle_request(
    mut request: Request<RecvStream>,
    mut respond: SendResponse<Bytes>,
) -> anyhow::Result<Duration> {
    let mut buf = ArrayVec::<u8, 16>::new();
    let start_time = Instant::now();
    let body = request.body_mut();

    while let Some(data) = body.data().await {
        let data = data.context("Не удалось извлечь данные")?;
        let len = data.len();
        if buf.len() + len <= buf.capacity() {
            buf.extend(data);
        } else {
            anyhow::bail!("Буфер слишком маленьний");
        }
        let _ = body.flow_control().release_capacity(len);
    }

    let mut message: Message = bincode::deserialize(&buf).context("Ошибка десериализации")?;
    if message.ty != MessageType::Ping {
        anyhow::bail!("Неверный тип сообщения");
    }

    tokio::time::sleep(Duration::from_millis(fastrand::u16(100..=500) as u64)).await;
    message.ty = MessageType::Pong;

    let response = http::Response::new(());
    let mut send = respond.send_response(response, false)?;
    send.send_data(
        Bytes::from(bincode::serialize(&message).expect("Ошибка сериализации")),
        true,
    )?;

    Ok(Instant::now().duration_since(start_time))
}
