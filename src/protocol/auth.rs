use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::protocol::{
    AuthenticationMessage, BackendMessage, ErrorResponse, FrontendMessage, PostgresCodec,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerAuth {
    Trust,
    CleartextPassword { password: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthResult {
    Ok,
    Failed,
}

pub async fn perform_authentication<S>(
    framed: &mut Framed<S, PostgresCodec>,
    auth: &ServerAuth,
    user: &str,
) -> Result<AuthResult>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    match auth {
        ServerAuth::Trust => {
            framed
                .send(BackendMessage::Authentication(AuthenticationMessage::Ok))
                .await?;
            Ok(AuthResult::Ok)
        }
        ServerAuth::CleartextPassword { password } => {
            framed
                .send(BackendMessage::Authentication(
                    AuthenticationMessage::CleartextPassword,
                ))
                .await?;

            let msg = match framed.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(AuthResult::Failed),
            };

            match msg {
                FrontendMessage::Password(pw) => {
                    if pw == *password {
                        framed
                            .send(BackendMessage::Authentication(AuthenticationMessage::Ok))
                            .await?;
                        Ok(AuthResult::Ok)
                    } else {
                        // Same SQLSTATE as PostgreSQL for invalid password.
                        let err = ErrorResponse::new(
                            "FATAL".to_string(),
                            "28P01".to_string(),
                            format!("password authentication failed for user \"{user}\""),
                        );
                        framed
                            .send(BackendMessage::ErrorResponse(Box::new(err)))
                            .await?;
                        Ok(AuthResult::Failed)
                    }
                }
                FrontendMessage::Terminate => Ok(AuthResult::Failed),
                other => {
                    let err = ErrorResponse::new(
                        "FATAL".to_string(),
                        "08P01".to_string(),
                        format!(
                            "Protocol error during authentication: expected PasswordMessage, got {other:?}"
                        ),
                    );
                    framed
                        .send(BackendMessage::ErrorResponse(Box::new(err)))
                        .await?;
                    Ok(AuthResult::Failed)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn build_startup_message(params: &[(&str, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196_608_i32.to_be_bytes()); // protocol 3.0
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);

        let len = (4 + body.len()) as i32;
        let mut msg = Vec::with_capacity(len as usize);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }

    fn build_password_message(password: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(password.as_bytes());
        payload.push(0);

        let len = (4 + payload.len()) as i32;
        let mut msg = Vec::with_capacity(1 + len as usize);
        msg.push(b'p');
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    async fn read_backend_message(stream: &mut tokio::io::DuplexStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).await.unwrap();
        let msg_type = header[0];
        let len = (&header[1..5]).get_i32() as usize;
        let mut payload = vec![0u8; len.saturating_sub(4)];
        stream.read_exact(&mut payload).await.unwrap();
        (msg_type, payload)
    }

    fn parse_error_fields(payload: &[u8]) -> std::collections::HashMap<u8, String> {
        let mut out = std::collections::HashMap::new();
        let mut buf = payload;
        while !buf.is_empty() && buf[0] != 0 {
            let field = buf[0];
            buf = &buf[1..];
            let null = buf.iter().position(|&b| b == 0).unwrap();
            let val = String::from_utf8_lossy(&buf[..null]).to_string();
            out.insert(field, val);
            buf = &buf[null + 1..];
        }
        out
    }

    #[tokio::test]
    async fn trust_sends_auth_ok() {
        let (mut client, server) = tokio::io::duplex(1024);

        let server_task = tokio::spawn(async move {
            let mut framed = Framed::new(server, PostgresCodec::new());
            match framed.next().await {
                Some(Ok(FrontendMessage::StartupMessage(_))) => {}
                other => panic!("unexpected startup decode: {other:?}"),
            }
            let res = perform_authentication(&mut framed, &ServerAuth::Trust, "alice").await;
            assert_eq!(res.unwrap(), AuthResult::Ok);
        });

        let startup = build_startup_message(&[("user", "alice"), ("database", "main")]);
        client.write_all(&startup).await.unwrap();

        let (typ, payload) = read_backend_message(&mut client).await;
        assert_eq!(typ, b'R');
        assert_eq!(payload.len(), 4);
        let auth = (&payload[..]).get_i32();
        assert_eq!(auth, 0);

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn password_requests_cleartext_then_ok_on_match() {
        let (mut client, server) = tokio::io::duplex(1024);

        let server_task = tokio::spawn(async move {
            let mut framed = Framed::new(server, PostgresCodec::new());
            match framed.next().await {
                Some(Ok(FrontendMessage::StartupMessage(_))) => {}
                other => panic!("unexpected startup decode: {other:?}"),
            }
            let auth = ServerAuth::CleartextPassword {
                password: "secret".to_string(),
            };
            let res = perform_authentication(&mut framed, &auth, "alice").await;
            assert_eq!(res.unwrap(), AuthResult::Ok);
        });

        let startup = build_startup_message(&[("user", "alice"), ("database", "main")]);
        client.write_all(&startup).await.unwrap();

        let (typ, payload) = read_backend_message(&mut client).await;
        assert_eq!(typ, b'R');
        let auth = (&payload[..]).get_i32();
        assert_eq!(auth, 3);

        let pw = build_password_message("secret");
        client.write_all(&pw).await.unwrap();

        let (typ, payload) = read_backend_message(&mut client).await;
        assert_eq!(typ, b'R');
        let auth = (&payload[..]).get_i32();
        assert_eq!(auth, 0);

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn password_sends_fatal_on_mismatch() {
        let (mut client, server) = tokio::io::duplex(1024);

        let server_task = tokio::spawn(async move {
            let mut framed = Framed::new(server, PostgresCodec::new());
            match framed.next().await {
                Some(Ok(FrontendMessage::StartupMessage(_))) => {}
                other => panic!("unexpected startup decode: {other:?}"),
            }
            let auth = ServerAuth::CleartextPassword {
                password: "secret".to_string(),
            };
            let res = perform_authentication(&mut framed, &auth, "alice").await;
            assert_eq!(res.unwrap(), AuthResult::Failed);
        });

        let startup = build_startup_message(&[("user", "alice"), ("database", "main")]);
        client.write_all(&startup).await.unwrap();

        let (typ, payload) = read_backend_message(&mut client).await;
        assert_eq!(typ, b'R');
        let auth = (&payload[..]).get_i32();
        assert_eq!(auth, 3);

        let pw = build_password_message("wrong");
        client.write_all(&pw).await.unwrap();

        let (typ, payload) = read_backend_message(&mut client).await;
        assert_eq!(typ, b'E');
        let fields = parse_error_fields(&payload);
        assert_eq!(fields.get(&b'S').unwrap(), "FATAL");
        assert_eq!(fields.get(&b'C').unwrap(), "28P01");

        server_task.await.unwrap();
    }
}
