use std::cmp::min;
use http::Request;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_HEADERS_SIZE: usize = 8000;
const MAX_BODY_SIZE: usize = 10000000;
const MAX_NUM_HEADERS: usize = 32;

#[derive(Debug)]
pub enum Error {
    IncompleteRequest(usize),
    MalformedRequest(httparse::Error),
    InvalidContentLength,
    ContentLengthMismatch,
    RequestBodyTooLarge,
    ConnectionError(std::io::Error),
}

fn get_content_length(req: &Request<Vec<u8>>) -> Result<Option<usize>, Error> {
    if let Some(header_value) = req.headers().get("content-length") {
        Ok(Some(
            header_value.to_str().or(Err(Error::InvalidContentLength))?.parse::<usize>().or(Err(Error::InvalidContentLength))?,
        ))
    } else {
        Ok(None)
    }
}

pub fn extended_header_value(req: &mut Request<Vec<u8>>, name: &'static str, extend_value: &str) {
    let value = match req.headers().get(name) {
        Some(existing_value) => {
            [existing_value.as_bytes(), b", ", extend_value.as_bytes()].concat()
        }
        None => extend_value.as_bytes().to_owned(),
    };

    req.headers_mut().insert(name, http::HeaderValue::from_bytes(&value).unwrap());
}

fn parse_request(buf: &[u8]) -> Result<Option<(Request<Vec<u8>>, usize)>, Error> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_NUM_HEADERS];
    let mut req = httparse::Request::new(&mut headers);
    let res = req.parse(buf).or_else(|e| Err(Error::MalformedRequest(e)))?;

    if let httparse::Status::Complete(len) = res {
        let mut request = http::Request::builder()
            .method(req.method.unwrap())
            .uri(req.path.unwrap())
            .version(http::Version::HTTP_11);
        for header in req.headers {
            request = request.header(header.name, header.value);
        }
        let request = request.body(Vec::new()).unwrap();
        Ok(Some((request, len)))
    } else {
        Ok(None)
    }
}

async fn read_headers(stream: &mut TcpStream) -> Result<Request<Vec<u8>>, Error> {
    let mut req_buffer = [0_u8; MAX_HEADERS_SIZE];
    let mut bytes_read = 0;
    loop {
        let new_bytes = stream
            .read(&mut req_buffer[bytes_read ..]).await
            .or_else(|e| Err(Error::ConnectionError(e)))?;
        if new_bytes == 0 {
            return Err(Error::IncompleteRequest(bytes_read));
        }

        bytes_read += new_bytes;

        if let Some((mut request, headers_len)) = parse_request(&req_buffer[..bytes_read])? {
            request.body_mut().extend_from_slice(&req_buffer[headers_len..bytes_read]);
            return Ok(request);
        }
    }
}

async fn read_body(stream: &mut TcpStream, req: &mut Request<Vec<u8>>, content_length: usize) -> Result<(), Error> {
    while req.body().len() < content_length {
        let mut buf = vec![0_u8; min(512, content_length)];
        let bytes_read = stream.read(&mut buf).await.or_else(|e| Err(Error::ConnectionError(e)))?;
        if bytes_read == 0 {
            log::debug!("Client sent more bytes than expected based on the given content length!");
            return Err(Error::ContentLengthMismatch);
        }
        req.body_mut().extend_from_slice(&buf[..bytes_read]);

    }
    Ok(())
}

pub async fn read_from_stream(stream: &mut TcpStream) -> Result<Request<Vec<u8>>, Error> {
    let mut req = read_headers(stream).await?;
    if let Some(content_length) = get_content_length(&req)? {
        if content_length > MAX_BODY_SIZE {
            return Err(Error::RequestBodyTooLarge);
        } else {
            read_body(stream, &mut req, content_length).await?;
        }
    }
    Ok(req)
}

pub async fn write_to_stream(req: &Request<Vec<u8>>, stream: &mut TcpStream) -> Result<(), std::io::Error> {
    stream.write(&format_request_line(req).into_bytes()).await?;
    stream.write(&['\r' as u8, '\n' as u8]).await?;
    for (header_name, header_value) in req.headers() {
        stream.write(&format!("{}: ", header_name).as_bytes()).await?;
        stream.write(header_value.as_bytes()).await?;
        stream.write(&['\r' as u8, '\n' as u8]).await?;
    }
    stream.write(&['\r' as u8, '\n' as u8]).await?;
    if req.body().len() > 0 {
        stream.write(req.body()).await?;
    }
    Ok(())
}

pub fn format_request_line(req: &Request<Vec<u8>>) -> String {
    format!("{} {} {:?}", req.method(), req.uri(), req.version())
}