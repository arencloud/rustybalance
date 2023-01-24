use clap::Parser;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::IpAddr;
use std::sync::Arc;
use rand::{Rng, SeedableRng};
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;
use tokio::sync::{RwLock, Mutex};
use tokio::time::{sleep, Duration};
use rustybalance::{request, response};

#[derive(Parser, Debug)]
#[clap(about = "Load balancing on rust")]
struct Cmd {
    /// IP:PORT binding
    #[clap(short = 'l', long, default_value = "0.0.0.0:8080")]
    bind: String,
    /// Backend hosts to forward requests
    #[clap(short, long)]
    backend: Vec<String>,
    /// Active health check every 10 seconds by default (interval in seconds)
    #[clap(short = 'i', long, default_value = "10")]
    active_health_check_interval: usize,
    /// Health check path
    #[clap(short = 'p', long, default_value = "/")]
    active_health_check_path: String,
    /// Maximum number of requests to accept per IP per minute (0 = unlimited)
    #[clap(long, short, default_value = "0")]
    max_requests_per_minute: usize,
}

struct LoadBalancerState {
    active_health_check_interval: usize,
    active_health_check_path: String,
    max_requests_per_minute: usize,
    backend_addresses: Vec<String>,
    backend_status: RwLock<(usize, Vec<bool>)>,
    rate_limit_counter: Mutex<HashMap<IpAddr, usize>>,
}

#[tokio::main]
async fn main() {
    if let Err(_) = std::env::var("RUSTYBALANCE_LOG") {
        std::env::set_var("RUSTYBALANCE_LOG", "debug");
    }
    pretty_env_logger::init();
    let cmd = Cmd::parse();
    if cmd.backend.len() < 1 {
        log::error!("At least one backend server must be specified using --backend option.");
        std::process::exit(1);
    }
    
    let mut listener = match TcpListener::bind(&cmd.bind).await {
        Ok(listener) => listener,
        Err(e) => {
            log::error!("Could not bind to {}: {}", cmd.bind, e);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", cmd.bind);

    let backend_nums = cmd.backend.len();
    let state = LoadBalancerState {
        active_health_check_interval: cmd.active_health_check_interval,
        active_health_check_path: cmd.active_health_check_path,
        max_requests_per_minute: cmd.max_requests_per_minute,
        backend_addresses: cmd.backend,
        backend_status: RwLock::new((backend_nums, vec![true;backend_nums])),
        rate_limit_counter: Mutex::new(HashMap::new()),
    };
    let shared_state = Arc::new(state);

    let state_ref_0 = shared_state.clone();

    tokio::spawn(async move {
        active_health_check(state_ref_0).await;
    });

    if shared_state.max_requests_per_minute > 0 {
        let state_ref_1 = shared_state.clone();
        tokio::spawn(async move {
            rate_limit_counter_updater(state_ref_1, 60).await;
        });
    }

    while let Some(stream) = listener.next().await {
        match stream {
            Ok(mut stream) => {
                if shared_state.max_requests_per_minute > 0 {
                    let mut rate_limit_counter = shared_state.rate_limit_counter.lock().await;
                    let ip_addr = stream.peer_addr().unwrap().ip();
                    let count = rate_limit_counter.entry(ip_addr).or_insert(0);
                    log::debug!("Address: {}, count: {}", ip_addr, count);
                    *count += 1;
                    if *count > shared_state.max_requests_per_minute {
                        let response = response::make_http_error(http::StatusCode::TOO_MANY_REQUESTS);
                        response::write_to_stream(&mut stream, &response).await.unwrap();
                        continue;
                    }
                }
                let shared_state_ref = shared_state.clone();
                tokio::spawn(async move {
                    connection_handler(stream, shared_state_ref).await;
                });
            },
            Err(_) => { break; }
        }
    }
}

async fn rate_limit_counter_updater(state: Arc<LoadBalancerState>, interval: u64) {
    sleep(Duration::from_secs(interval)).await;
    let mut rate_limit_counter = state.rate_limit_counter.lock().await;
    rate_limit_counter.clear();
}

async fn check_service(state: &Arc<LoadBalancerState>, idx: usize, path: &String) -> Option<usize> {
    let backend_addr = &state.backend_addresses[idx];
    let mut stream = connect_to_backend(idx, &state).await.ok()?;
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(path)
        .header("Host", backend_addr)
        .body(Vec::new())
        .unwrap();

    let _ = request::write_to_stream(&request, &mut stream).await.ok()?;
    let result = response::read_from_stream(&mut stream, &http::Method::GET).await.ok()?;
    if result.status().as_u16() != 200 {
        return None;
    } else {
        return Some(1);
    }
}

async fn connect_to_backend(backend_idx: usize, state: &Arc<LoadBalancerState>) -> Result<TcpStream, std::io::Error> {
    let backend_addr = &state.backend_addresses[backend_idx];
    return match TcpStream::connect(backend_addr).await {
        Ok(steam) => Ok(steam),
        Err(e) => {
            log::error!("Failed to connect to backend {}: {}", backend_addr, e);
            Err(e)
        }
    }
}


async fn connect_to_random_backend(state: Arc<LoadBalancerState>) -> Result<TcpStream, std::io::Error> {
    loop {
        if let Some(backend_idx) = pick_random_alive_backend(&state).await {
            match connect_to_backend(backend_idx, &state).await {
                Ok(stream) => return Ok(stream),
                Err(_) => {
                    let mut backend_status_mut = state.backend_status.write().await;
                    backend_status_mut.0 -= 1;
                    backend_status_mut.1[backend_idx] = false;
                }
            }
        } else {
            return Err(std::io::Error::new(ErrorKind::Other, "All backend servers are down!"))
        }
    }
}

async fn pick_random_alive_backend(state: &Arc<LoadBalancerState>) -> Option<usize> {
    let mut random_num = rand::rngs::StdRng::from_entropy();
    let backend_status_view = state.backend_status.read().await;
    if backend_status_view.0 == 0 {
        return None;
    }
    let mut backend_idx;
    loop {
        backend_idx = random_num.gen_range(0..state.backend_addresses.len());
        if backend_status_view.1[backend_idx] {
            return Some(backend_idx);
        }
    }
}

async fn send_response(client_connection: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_addr = client_connection.peer_addr().unwrap().ip().to_string();
    log::info!("{} <- {}", client_addr, response::format_response_line(&response));
    if let Err(e) = response::write_to_stream(client_connection, &response).await {
        log::warn!("Failed to send response to client: {}", e);
        return;
    }
}

async fn active_health_check(state: Arc<LoadBalancerState>) {
    let interval = state.active_health_check_interval as u64;
    let path = &state.active_health_check_path;
    loop {
        sleep(Duration::from_secs(interval)).await;
        let mut backend_status_mut = state.backend_status.write().await;
        for idx in 0..backend_status_mut.1.len() {
            if check_service(&state, idx, path).await.is_some() {
                if backend_status_mut.1[idx] {
                    backend_status_mut.0 += 1;
                    backend_status_mut.1[idx] = true;
                }
            } else {
                if backend_status_mut.1[idx] {
                    backend_status_mut.0 -= 1;
                    backend_status_mut.1[idx] = false;
                }
            }
        }
    }
}

async fn connection_handler(mut client_connection: TcpStream, state: Arc<LoadBalancerState>) {
    let client_addr = client_connection.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_addr);
    let mut backend_connection = match connect_to_random_backend(state).await {
        Ok(stream) => stream,
        Err(_e) => {
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_connection, &response).await;
            return;
        }
    };

    let backend_addr = client_connection.peer_addr().unwrap().ip().to_string();

    loop {
        let mut request = match request::read_from_stream(&mut client_connection).await {
            Ok(request) => request,
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(e) => {
                log::debug!("Error parsing request: {:?}", e);
                let response = response::make_http_error(match e {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_connection, &response).await;
                continue;
            }
        };
        log::info!("{} -> {}: {}", client_addr, backend_addr, request::format_request_line(&request));
        request::extended_header_value(&mut request, "x-forwarded-for", &client_addr);

        if let Err(e) = request::write_to_stream(&request, &mut backend_connection).await {
            log::error!("Failed to send request to backend {}: {}", backend_addr, e);
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_connection, &response).await;
            return;
        }
        log::debug!("Forwarded request to server");
        let response = match response::read_from_stream(&mut backend_connection, request.method()).await {
            Ok(response) => response,
            Err(e) => {
                log::error!("Error reading response from server: {:?}", e);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_connection, &response).await;
                return;
            }
        };
        send_response(&mut client_connection, &response).await;
        log::debug!("Forwarded response to client");
    }
}
