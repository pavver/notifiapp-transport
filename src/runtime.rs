//! Async runtime abstractions for WASM compatibility.
use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
pub use tokio::time::{Duration, Instant, sleep, sleep_until, timeout};

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn<F>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(f)
}

#[cfg(target_arch = "wasm32")]
pub use web_time::{Duration, Instant};

#[cfg(target_arch = "wasm32")]
pub fn spawn<F>(f: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(f)
}

#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: Duration) {
    gloo_timers::future::sleep(duration).await;
}

#[cfg(target_arch = "wasm32")]
pub async fn sleep_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now {
        gloo_timers::future::sleep(deadline - now).await;
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug)]
pub struct Elapsed;

#[cfg(target_arch = "wasm32")]
pub async fn timeout<T, F: Future<Output = T>>(duration: Duration, f: F) -> Result<T, Elapsed> {
    let sleep_fut = gloo_timers::future::sleep(duration);
    tokio::select! {
        res = f => Ok(res),
        _ = sleep_fut => Err(Elapsed),
    }
}
