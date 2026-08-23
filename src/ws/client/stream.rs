#[derive(Debug)]
pub enum WsMessage {
    Text(String),
    Binary(Vec<u8>),
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::WsMessage;
    use futures_util::{Sink, Stream};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::net::TcpStream;
    use tokio_tungstenite::MaybeTlsStream;
    use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::protocol::Message};

    type InnerStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

    pub struct WsStream {
        inner: InnerStream,
    }

    impl WsStream {
        pub async fn connect(url: &str) -> Result<Self, String> {
            let (ws_stream, _) = connect_async(url).await.map_err(|e| e.to_string())?;
            Ok(Self { inner: ws_stream })
        }

        pub async fn close(&mut self) -> Result<(), String> {
            self.inner.close(None).await.map_err(|e| e.to_string())
        }
    }

    impl Stream for WsStream {
        type Item = Result<WsMessage, String>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => match msg {
                    Message::Text(t) => Poll::Ready(Some(Ok(WsMessage::Text(t.to_string())))),
                    Message::Binary(b) => Poll::Ready(Some(Ok(WsMessage::Binary(b.to_vec())))),
                    Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => {
                        // Skip or handle ping/pong if necessary. Tungstenite auto-replies to pings.
                        // Let's just return Pending for now and wake up, or actually loop.
                        // Wait, it's easier to just map it or ignore it.
                        // For simplicity, return an empty binary message which the accumulator will ignore,
                        // or we can loop, but poll_next shouldn't loop without yielding.
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                },
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.to_string()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl Sink<WsMessage> for WsStream {
        type Error = String;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.inner)
                .poll_ready(cx)
                .map_err(|e| e.to_string())
        }

        fn start_send(mut self: Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
            let msg = match item {
                WsMessage::Text(t) => Message::Text(t.into()),
                WsMessage::Binary(b) => Message::Binary(b.into()),
            };
            Pin::new(&mut self.inner)
                .start_send(msg)
                .map_err(|e| e.to_string())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.inner)
                .poll_flush(cx)
                .map_err(|e| e.to_string())
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.inner)
                .poll_close(cx)
                .map_err(|e| e.to_string())
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::WsMessage;
    use futures_util::{Sink, Stream};
    use gloo_net::websocket::Message;
    use gloo_net::websocket::futures::WebSocket;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    pub struct WsStream {
        inner: WebSocket,
    }

    impl WsStream {
        pub async fn connect(url: &str) -> Result<Self, String> {
            match WebSocket::open(url) {
                Ok(ws) => Ok(Self { inner: ws }),
                Err(e) => Err(e.to_string()),
            }
        }
        pub async fn close(&mut self) -> Result<(), String> {
            Ok(())
        }
    }

    impl Stream for WsStream {
        type Item = Result<WsMessage, String>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => match msg {
                    Message::Text(t) => Poll::Ready(Some(Ok(WsMessage::Text(t)))),
                    Message::Bytes(b) => Poll::Ready(Some(Ok(WsMessage::Binary(b)))),
                },
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.to_string()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl Sink<WsMessage> for WsStream {
        type Error = String;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.inner)
                .poll_ready(cx)
                .map_err(|e| e.to_string())
        }

        fn start_send(mut self: Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
            let msg = match item {
                WsMessage::Text(t) => Message::Text(t),
                WsMessage::Binary(b) => Message::Bytes(b),
            };
            Pin::new(&mut self.inner)
                .start_send(msg)
                .map_err(|e| e.to_string())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.inner)
                .poll_flush(cx)
                .map_err(|e| e.to_string())
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.inner)
                .poll_close(cx)
                .map_err(|e| e.to_string())
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::WsStream;

#[cfg(target_arch = "wasm32")]
pub use wasm::WsStream;
