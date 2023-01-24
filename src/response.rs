use http::{Response, Version, StatusCode};
use httparse::Status;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_HEADERS_SIZE: usize = 8000;
const MAX_BODY_SIZE: usize = 10000000;
const MAX_NUM_HEADERS: usize = 32;

#[derive(Debug)]
pub enum Error {
    IncompleteResponse,
    MalformedResponse(httparse::Error),
    InvalidContentLength,
    ContentLengthMismatch,
    ResponseBodyTooLarge,
    ConnectionError(std::io::Error),
}

fn get_content_length(res: &Response<Vec<u8>>) -> Result<Option<usize>, Error> {
    if let Some(header_value_response) = res.headers().get("content-length") {
        Ok(Some(
            header_value_response.to_str().or(Err(Error::InvalidContentLength))?.parse::<usize>().or(Err(Error::InvalidContentLength))?,
        ))
    } else {
        Ok(None)
    }
}

fn parse_response(buf: &[u8]) -> Result<Option<(Response<Vec<u8>>, usize)>, Error> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_NUM_HEADERS];
    let mut resp = httparse::Response::new(&mut headers);
    let res = resp.parse(buf).or_else(|e| Err(Error::MalformedResponse(e)))?;

    if let Status::Complete(len) = res {
        let mut response = http::Response::builder()
            .status(resp.code.unwrap())
            .version(Version::HTTP_11);
        for header in resp.headers {
            response = response.header(header.name, header.value);
        }
        let response = response.body(Vec::new()).unwrap();
        Ok(Some((response, len)))
    } else {
        Ok(None)
    }
}

async fn read_headers(stream: &mut TcpStream) -> Result<Response<Vec<u8>>, Error> {
    let mut resp_buffer = [0_u8; MAX_HEADERS_SIZE];
    let mut bytes_read = 0;
    loop {
        let new_bytes = stream
            .read(&mut resp_buffer[bytes_read ..]).await
            .or_else(|e| Err(Error::ConnectionError(e)))?;
        if new_bytes == 0 {
            return Err(Error::IncompleteResponse);
        }

        bytes_read += new_bytes;

        if let Some((mut response, headers_len)) = parse_response(&resp_buffer[..bytes_read])? {
            response.body_mut().extend_from_slice(&resp_buffer[headers_len..bytes_read]);
            return Ok(response);
        }
    }
}

async fn read_body(stream: &mut TcpStream, resp: &mut Response<Vec<u8>>) -> Result<(), Error> {
    let content_length = get_content_length(resp)?;
    while content_length.is_none() || resp.body().len() < content_length.unwrap() {
        let mut buf = vec![0_u8; 512];
        let bytes_read = stream.read(&mut buf).await.or_else(|e| Err(Error::ConnectionError(e)))?;
        if bytes_read == 0 {
            if content_length.is_none() {
                break;
            } else {
                return Err(Error::ContentLengthMismatch);
            }
        }
        if content_length.is_some() && resp.body().len() + bytes_read > content_length.unwrap() {
            return Err(Error::ContentLengthMismatch);
        }
        if resp.body().len() + bytes_read > MAX_BODY_SIZE {
            return Err(Error::ResponseBodyTooLarge)
        }
        resp.body_mut().extend_from_slice(&buf[..bytes_read]);
    }
    Ok(())
}

pub async fn read_from_stream(stream: &mut TcpStream, req_method: &http::Method,) -> Result<Response<Vec<u8>>, Error> {
    let mut response = read_headers(stream).await?;
    if !(req_method == http::Method::HEAD
       || response.status().as_u16() < 200
       || response.status() == StatusCode::NO_CONTENT
       || response.status() == StatusCode::NOT_MODIFIED) {
        read_body(stream, &mut response).await?;
    }
    Ok(response)
}

pub async fn write_to_stream(stream: &mut TcpStream, resp: &Response<Vec<u8>>) -> Result<(), std::io::Error> {
    stream.write(&format_response_line(resp).into_bytes()).await?;
    stream.write(&['\r' as u8, '\n' as u8]).await?;
    for (header_name, header_value) in resp.headers() {
        stream.write(&format!("{}: ", header_name).as_bytes()).await?;
        stream.write(header_value.as_bytes()).await?;
        stream.write(&['\r' as u8, '\n' as u8]).await?;
    }

    stream.write(&['\r' as u8, '\n' as u8]).await?;
    if resp.body().len() > 0 {
        stream.write(resp.body()).await?;
    }
    Ok(())
}

pub fn format_response_line(resp: &Response<Vec<u8>>) -> String {
    format!("{:?} {} {}", resp.version(), resp.status().as_str(), resp.status().canonical_reason().unwrap_or(""))
}

pub fn make_http_error(status: StatusCode) -> Response<Vec<u8>> {
    let body = format!(
        "HTTP {} {}", status.as_u16(), status.canonical_reason().unwrap_or("")
    ).into_bytes();
    Response::builder()
        .status(status)
        .header("Content-Type", "text/plain")
        .header("Content-Length", body.len().to_string())
        .version(Version::HTTP_11)
        .body(body)
        .unwrap()
}