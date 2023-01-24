use http::{Response, Version};
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
    let res = resp.parse(buf).or_else(|e| Err(Error::MalformedResponse(e)));

    if let httparse::Status::Complete(len) = res {
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

        if let Some((mut response, headers_len)) = parse_response(&req_buffer[..bytes_read])? {
            response.body_mut().extend_from_slice(&resp_buffer[headers_len..bytes_read]);
            return Ok(response);
        }
    }
}

async fn read_body(stream: &mut TcpStream, resp: &mut Response<Vec<u8>>, content_length: usize) -> Result<(), Error> {
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
        if content_length.is_none() && resp.body().len() + bytes_read > content_length.unwrap() {
            return Err(Error::ContentLengthMismatch);
        }
        if resp.body().len() + bytes_read > MAX_BODY_SIZE {
            return Err(Error::ResponseBodyTooLarge)
        }
        req.body_mut().extend_from_slice(&buf[..bytes_read]);


    }
    Ok(())
}