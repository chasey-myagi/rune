use crate::rune::{RuneContext, RuneError, RuneHandler, StreamRuneHandler, StreamSender};
use crate::trace::TRACE_ID_KEY;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::Instrument;

/// Invoker that executes a rune, either locally or remotely.
///
/// # Cancellation safety
/// Implementations of `invoke_stream` MUST be cancellation-safe: when the
/// returned `Future` is dropped (e.g. because the client disconnected), the
/// handler must not leak resources or leave partial state behind. The runtime
/// relies on `Future::drop` for cleanup; there is no explicit "cancel"
/// signal.
#[async_trait::async_trait]
pub trait RuneInvoker: Send + Sync {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError>;
    async fn invoke_stream(
        &self,
        ctx: RuneContext,
        input: Bytes,
    ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError>;
}

/// 进程内调用——直接调 handler
pub struct LocalInvoker {
    handler: RuneHandler,
}

impl LocalInvoker {
    pub fn new(handler: RuneHandler) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl RuneInvoker for LocalInvoker {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        (self.handler)(ctx, input).await
    }

    async fn invoke_stream(
        &self,
        ctx: RuneContext,
        input: Bytes,
    ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
        // Intentional fallback: this rune was registered with a unary handler,
        // not a StreamRuneHandler. Callers routing stream requests here must
        // accept single-chunk responses.
        tracing::debug!(rune = %ctx.rune_name, "invoke_stream on unary handler — degrading to single-chunk stream");
        let result = (self.handler)(ctx, input).await;
        let (tx, rx) = mpsc::channel(1);
        let _ = tx.send(result).await;
        Ok(rx)
    }
}

/// Local stream invoker — wraps a StreamRuneHandler
pub struct LocalStreamInvoker {
    handler: Arc<dyn StreamRuneHandler>,
}

impl LocalStreamInvoker {
    pub fn new(handler: Arc<dyn StreamRuneHandler>) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl RuneInvoker for LocalStreamInvoker {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        let mut rx = self.invoke_stream(ctx, input).await?;
        let mut collected = Vec::new();
        while let Some(chunk) = rx.recv().await {
            collected.push(chunk?);
        }
        // CONTRACT: stream handlers emit the *meaningful* result as their final
        // chunk (e.g. tx.emit(data).await followed by tx.end().await). Earlier
        // chunks are treated as intermediate and DISCARDED here.
        //
        // This is a convention, not enforced by the type system. If a handler
        // places meaningful data in non-final chunks, that data is lost.
        // Callers that need every chunk must use `invoke_stream` directly.
        Ok(collected.into_iter().last().unwrap_or_default())
    }

    async fn invoke_stream(
        &self,
        ctx: RuneContext,
        input: Bytes,
    ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
        let (tx, rx) = mpsc::channel(32);
        let sender = StreamSender::new(tx.clone());
        let handler = Arc::clone(&self.handler);
        tokio::spawn(async move {
            tokio::select! {
                result = handler.execute(ctx, input, sender) => {
                    if let Err(e) = result {
                        tracing::debug!("stream handler stopped: {}", e);
                    }
                }
                // Cancel the handler if the receiver is dropped before completion.
                _ = tx.closed() => {}
            }
        });
        Ok(rx)
    }
}

/// 远程调用——通过 SessionManager 发 gRPC
pub struct RemoteInvoker {
    pub(crate) session: Arc<crate::session::SessionManager>,
    pub(crate) caster_id: String,
}

#[async_trait::async_trait]
impl RuneInvoker for RemoteInvoker {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        let request_id = ctx.request_id.clone();
        let rune_name = ctx.rune_name.clone();
        let trace_id = ctx.context.get(TRACE_ID_KEY).cloned().unwrap_or_default();
        self.session
            .execute(
                &self.caster_id,
                &request_id,
                &rune_name,
                input,
                ctx.context.clone(),
                ctx.timeout,
            )
            .instrument(tracing::info_span!(
                "remote.invoke",
                trace_id = %trace_id,
                request_id = %request_id,
                caster_id = %self.caster_id,
                rune_name = %rune_name
            ))
            .await
    }

    async fn invoke_stream(
        &self,
        ctx: RuneContext,
        input: Bytes,
    ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
        let request_id = ctx.request_id.clone();
        let rune_name = ctx.rune_name.clone();
        let trace_id = ctx.context.get(TRACE_ID_KEY).cloned().unwrap_or_default();
        self.session
            .execute_stream(
                &self.caster_id,
                &request_id,
                &rune_name,
                input,
                ctx.context.clone(),
                ctx.timeout,
            )
            .instrument(tracing::info_span!(
                "remote.invoke_stream",
                trace_id = %trace_id,
                request_id = %request_id,
                caster_id = %self.caster_id,
                rune_name = %rune_name
            ))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rune::{make_handler, RuneContext, RuneError, StreamRuneHandler, StreamSender};
    use bytes::Bytes;
    use std::time::Duration;

    fn test_ctx() -> RuneContext {
        RuneContext {
            rune_name: "test".into(),
            request_id: "r-1".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
            disable_runtime_retry: false,
        }
    }

    // ========================================================================
    // LocalInvoker — invoke_once
    // ========================================================================

    #[tokio::test]
    async fn local_invoke_once_returns_handler_output() {
        let handler = make_handler(|_ctx, input| async move {
            let mut out = b"echo:".to_vec();
            out.extend_from_slice(&input);
            Ok(Bytes::from(out))
        });
        let invoker = LocalInvoker::new(handler);
        let result = invoker
            .invoke_once(test_ctx(), Bytes::from("hello"))
            .await
            .unwrap();
        assert_eq!(result, Bytes::from("echo:hello"));
    }

    #[tokio::test]
    async fn local_invoke_once_propagates_handler_error() {
        let handler = make_handler(|_ctx, _input| async move {
            Err(RuneError::ExecutionFailed {
                code: "E001".into(),
                message: "something broke".into(),
            })
        });
        let invoker = LocalInvoker::new(handler);
        let result = invoker.invoke_once(test_ctx(), Bytes::from("data")).await;
        match result {
            Err(RuneError::ExecutionFailed { code, message }) => {
                assert_eq!(code, "E001");
                assert_eq!(message, "something broke");
            }
            other => panic!("expected ExecutionFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn local_invoke_once_empty_input() {
        let handler =
            make_handler(
                |_ctx, input| async move { Ok(Bytes::from(format!("len={}", input.len()))) },
            );
        let invoker = LocalInvoker::new(handler);
        let result = invoker.invoke_once(test_ctx(), Bytes::new()).await.unwrap();
        assert_eq!(result, Bytes::from("len=0"));
    }

    #[tokio::test]
    async fn local_invoke_once_large_input_1mb() {
        let big = Bytes::from(vec![0x42u8; 1_000_000]);
        let handler =
            make_handler(
                |_ctx, input| async move { Ok(Bytes::from(format!("size={}", input.len()))) },
            );
        let invoker = LocalInvoker::new(handler);
        let result = invoker.invoke_once(test_ctx(), big).await.unwrap();
        assert_eq!(result, Bytes::from("size=1000000"));
    }

    #[tokio::test]
    async fn local_invoke_once_passes_context_fields() {
        let handler = make_handler(|ctx, _input| async move {
            Ok(Bytes::from(format!("{}:{}", ctx.rune_name, ctx.request_id)))
        });
        let invoker = LocalInvoker::new(handler);
        let ctx = RuneContext {
            rune_name: "my_rune".into(),
            request_id: "req-42".into(),
            context: Default::default(),
            timeout: Duration::from_secs(5),
            disable_runtime_retry: false,
        };
        let result = invoker.invoke_once(ctx, Bytes::new()).await.unwrap();
        assert_eq!(result, Bytes::from("my_rune:req-42"));
    }

    // ========================================================================
    // LocalInvoker — invoke_stream (fallback: wraps unary as single-item)
    // ========================================================================

    #[tokio::test]
    async fn local_invoke_stream_returns_single_chunk_from_unary() {
        let handler = make_handler(|_ctx, _input| async move { Ok(Bytes::from("only-chunk")) });
        let invoker = LocalInvoker::new(handler);
        let mut rx = invoker
            .invoke_stream(test_ctx(), Bytes::new())
            .await
            .unwrap();

        let chunk = rx.recv().await.expect("should receive one chunk");
        assert_eq!(chunk.unwrap(), Bytes::from("only-chunk"));

        // Channel closed after single item
        assert!(
            rx.recv().await.is_none(),
            "stream should end after one chunk"
        );
    }

    #[tokio::test]
    async fn local_invoke_stream_propagates_unary_error() {
        let handler =
            make_handler(
                |_ctx, _input| async move { Err(RuneError::InvalidInput("bad data".into())) },
            );
        let invoker = LocalInvoker::new(handler);
        let mut rx = invoker
            .invoke_stream(test_ctx(), Bytes::new())
            .await
            .unwrap();

        let chunk = rx.recv().await.expect("should receive error chunk");
        assert!(matches!(chunk, Err(RuneError::InvalidInput(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_invoke_once_concurrent_10_calls() {
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let invoker = Arc::new(LocalInvoker::new(handler));

        let mut handles = Vec::new();
        for i in 0..10 {
            let inv = Arc::clone(&invoker);
            let ctx = RuneContext {
                rune_name: "echo".into(),
                request_id: format!("r-{}", i),
                context: Default::default(),
                timeout: Duration::from_secs(30),
                disable_runtime_retry: false,
            };
            let payload = Bytes::from(format!("msg-{}", i));
            handles.push(tokio::spawn(
                async move { inv.invoke_once(ctx, payload).await },
            ));
        }

        for (i, h) in handles.into_iter().enumerate() {
            let result = h.await.unwrap().unwrap();
            assert_eq!(result, Bytes::from(format!("msg-{}", i)));
        }
    }

    // ========================================================================
    // LocalStreamInvoker
    // ========================================================================

    struct MultiChunkHandler;

    #[async_trait::async_trait]
    impl StreamRuneHandler for MultiChunkHandler {
        async fn execute(
            &self,
            _ctx: RuneContext,
            _input: Bytes,
            tx: StreamSender,
        ) -> Result<(), RuneError> {
            tx.emit(Bytes::from("chunk-1")).await?;
            tx.emit(Bytes::from("chunk-2")).await?;
            tx.emit(Bytes::from("chunk-3")).await?;
            tx.end().await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn stream_invoker_invoke_stream_returns_multiple_chunks() {
        let invoker = LocalStreamInvoker::new(Arc::new(MultiChunkHandler));
        let mut rx = invoker
            .invoke_stream(test_ctx(), Bytes::new())
            .await
            .unwrap();

        let c1 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c1, Bytes::from("chunk-1"));
        let c2 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c2, Bytes::from("chunk-2"));
        let c3 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c3, Bytes::from("chunk-3"));

        // Stream ends
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn stream_invoker_invoke_once_returns_last_chunk() {
        let invoker = LocalStreamInvoker::new(Arc::new(MultiChunkHandler));
        let result = invoker.invoke_once(test_ctx(), Bytes::new()).await.unwrap();
        assert_eq!(
            result,
            Bytes::from("chunk-3"),
            "invoke_once should return last chunk"
        );
    }

    struct EmptyStreamHandler;

    #[async_trait::async_trait]
    impl StreamRuneHandler for EmptyStreamHandler {
        async fn execute(
            &self,
            _ctx: RuneContext,
            _input: Bytes,
            tx: StreamSender,
        ) -> Result<(), RuneError> {
            tx.end().await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn stream_invoker_invoke_stream_zero_chunks() {
        let invoker = LocalStreamInvoker::new(Arc::new(EmptyStreamHandler));
        let mut rx = invoker
            .invoke_stream(test_ctx(), Bytes::new())
            .await
            .unwrap();
        // Should close immediately with no data
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn stream_invoker_invoke_once_empty_stream_returns_default() {
        let invoker = LocalStreamInvoker::new(Arc::new(EmptyStreamHandler));
        let result = invoker.invoke_once(test_ctx(), Bytes::new()).await.unwrap();
        assert_eq!(
            result,
            Bytes::new(),
            "empty stream should return Bytes::default()"
        );
    }

    struct ErrorStreamHandler;

    #[async_trait::async_trait]
    impl StreamRuneHandler for ErrorStreamHandler {
        async fn execute(
            &self,
            _ctx: RuneContext,
            _input: Bytes,
            tx: StreamSender,
        ) -> Result<(), RuneError> {
            tx.emit(Bytes::from("ok-chunk")).await?;
            Err(RuneError::ExecutionFailed {
                code: "STREAM_ERR".into(),
                message: "mid-stream failure".into(),
            })
        }
    }

    #[tokio::test]
    async fn stream_invoker_handler_error_after_emit() {
        let invoker = LocalStreamInvoker::new(Arc::new(ErrorStreamHandler));
        let mut rx = invoker
            .invoke_stream(test_ctx(), Bytes::new())
            .await
            .unwrap();

        // First chunk should arrive
        let c1 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c1, Bytes::from("ok-chunk"));

        // Stream ends (error in handler drops the sender, no more items)
        // The error is logged but the channel just closes
        assert!(rx.recv().await.is_none());
    }

    struct EchoStreamHandler;

    #[async_trait::async_trait]
    impl StreamRuneHandler for EchoStreamHandler {
        async fn execute(
            &self,
            ctx: RuneContext,
            input: Bytes,
            tx: StreamSender,
        ) -> Result<(), RuneError> {
            // Echo back the input along with context
            tx.emit(Bytes::from(format!("name={}", ctx.rune_name)))
                .await?;
            tx.emit(Bytes::from(format!("input_len={}", input.len())))
                .await?;
            tx.end().await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn stream_invoker_passes_context_and_input() {
        let invoker = LocalStreamInvoker::new(Arc::new(EchoStreamHandler));
        let ctx = RuneContext {
            rune_name: "stream_test".into(),
            request_id: "r-99".into(),
            context: Default::default(),
            timeout: Duration::from_secs(10),
            disable_runtime_retry: false,
        };
        let mut rx = invoker
            .invoke_stream(ctx, Bytes::from("payload"))
            .await
            .unwrap();

        let c1 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c1, Bytes::from("name=stream_test"));
        let c2 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c2, Bytes::from("input_len=7"));
        assert!(rx.recv().await.is_none());
    }

    // ========================================================================
    // RemoteInvoker — uses SessionManager (unit-test level)
    // ========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_invoker_invoke_once_delegates_to_session() {
        use crate::session::SessionManager;
        use crate::session::{CasterState, PendingRequest, PendingResponse};
        use dashmap::DashMap;
        use tokio::sync::Semaphore;

        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let (tx, _rx) = mpsc::channel(16);
        let semaphore = Arc::new(Semaphore::new(5));
        let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());

        mgr.sessions.insert(
            "caster-1".to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::clone(&pending),
                timeout_handles: Arc::new(dashmap::DashMap::new()),
                semaphore: Arc::clone(&semaphore),
                generation: 1,
            },
        );

        let invoker = RemoteInvoker {
            session: Arc::clone(&mgr),
            caster_id: "caster-1".to_string(),
        };

        // Spawn invoke_once which will block waiting for response
        let handle = tokio::spawn(async move {
            invoker
                .invoke_once(
                    RuneContext {
                        rune_name: "remote_rune".into(),
                        request_id: "req-remote-1".into(),
                        context: Default::default(),
                        timeout: Duration::from_secs(30),
                        disable_runtime_retry: false,
                    },
                    Bytes::from("remote_input"),
                )
                .await
        });

        // Wait for the pending entry
        for _ in 0..100 {
            if pending.contains_key("req-remote-1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            pending.contains_key("req-remote-1"),
            "request should appear in pending"
        );

        // Simulate response
        if let Some((_, p)) = pending.remove("req-remote-1") {
            semaphore.add_permits(1);
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Ok(Bytes::from("remote_result")));
                }
                _ => panic!("expected Once"),
            }
        }

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, Bytes::from("remote_result"));
    }

    #[tokio::test]
    async fn remote_invoker_invoke_once_nonexistent_caster() {
        use crate::session::SessionManager;

        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let invoker = RemoteInvoker {
            session: mgr,
            caster_id: "no-such-caster".to_string(),
        };
        let result = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        assert!(matches!(result, Err(RuneError::Unavailable)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_invoker_invoke_stream_delegates_to_session() {
        use crate::session::SessionManager;
        use crate::session::{CasterState, PendingRequest, PendingResponse};
        use dashmap::DashMap;
        use tokio::sync::Semaphore;

        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let (tx, _rx) = mpsc::channel(16);
        let semaphore = Arc::new(Semaphore::new(5));
        let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());

        mgr.sessions.insert(
            "caster-s".to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::clone(&pending),
                timeout_handles: Arc::new(dashmap::DashMap::new()),
                semaphore: Arc::clone(&semaphore),
                generation: 1,
            },
        );

        let invoker = RemoteInvoker {
            session: Arc::clone(&mgr),
            caster_id: "caster-s".to_string(),
        };

        let handle = tokio::spawn(async move {
            invoker
                .invoke_stream(
                    RuneContext {
                        rune_name: "stream_rune".into(),
                        request_id: "req-stream-1".into(),
                        context: Default::default(),
                        timeout: Duration::from_secs(30),
                        disable_runtime_retry: false,
                    },
                    Bytes::from("data"),
                )
                .await
        });

        for _ in 0..100 {
            if pending.contains_key("req-stream-1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pending.contains_key("req-stream-1"));

        // Send stream data then close
        if let Some((_, p)) = pending.remove("req-stream-1") {
            semaphore.add_permits(1);
            match p.tx {
                PendingResponse::Stream(tx) => {
                    let _ = tx.send(Ok(Bytes::from("s-chunk-1"))).await;
                    let _ = tx.send(Ok(Bytes::from("s-chunk-2"))).await;
                }
                _ => panic!("expected Stream"),
            }
        }

        let mut rx = handle.await.unwrap().unwrap();
        let c1 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c1, Bytes::from("s-chunk-1"));
        let c2 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c2, Bytes::from("s-chunk-2"));
    }
}
