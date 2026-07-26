use notifiapp_transport::ws::{
    BackoffStrategy, ConstantBackoff, ExponentialBackoff, LinearBackoff,
};
use std::time::Duration;

#[test]
fn test_constant_backoff() {
    let strategy = ConstantBackoff::new(Duration::from_secs(5));
    assert_eq!(strategy.next_delay(0), Duration::from_secs(5));
    assert_eq!(strategy.next_delay(1), Duration::from_secs(5));
    assert_eq!(strategy.next_delay(10), Duration::from_secs(5));
}

#[test]
fn test_linear_backoff() {
    let strategy = LinearBackoff::new(
        Duration::from_secs(2),
        Duration::from_secs(3),
        Duration::from_secs(10),
    );
    assert_eq!(strategy.next_delay(0), Duration::from_secs(0));
    assert_eq!(strategy.next_delay(1), Duration::from_secs(2)); // initial
    assert_eq!(strategy.next_delay(2), Duration::from_secs(5)); // initial + step
    assert_eq!(strategy.next_delay(3), Duration::from_secs(8)); // initial + 2*step
    assert_eq!(strategy.next_delay(4), Duration::from_secs(10)); // max delay capped
    assert_eq!(strategy.next_delay(10), Duration::from_secs(10)); // capped
}

#[test]
fn test_exponential_backoff() {
    let strategy = ExponentialBackoff::new(Duration::from_secs(2), 2.0, Duration::from_secs(15));
    assert_eq!(strategy.next_delay(0), Duration::from_secs(0));
    assert_eq!(strategy.next_delay(1), Duration::from_secs(2)); // 2 * 2^0
    assert_eq!(strategy.next_delay(2), Duration::from_secs(4)); // 2 * 2^1
    assert_eq!(strategy.next_delay(3), Duration::from_secs(8)); // 2 * 2^2
    assert_eq!(strategy.next_delay(4), Duration::from_secs(15)); // capped (16 -> 15)
    assert_eq!(strategy.next_delay(10), Duration::from_secs(15)); // capped
}
