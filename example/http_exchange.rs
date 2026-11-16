use rendezqueue::{Mode, http::HttpClient};
use std::{
    env,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn main() -> Result<(), Box<dyn Error>> {
    let url = env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:15050/".into());
    // A new room on every demo run avoids accidentally joining old retained state.
    // This is demonstration uniqueness, not a cryptographic production allocator.
    let room = format!(
        "demo-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let room = room.into_bytes();
    let mut alice = HttpClient::new(
        b"alice".to_vec(),
        &url,
        room.clone(),
        Duration::from_secs(5),
    )?;
    let mut bob = HttpClient::new(b"bob".to_vec(), &url, room, Duration::from_secs(5))?;
    println!("Room endpoint: {url}");
    alice.step(Mode::Current)?;
    bob.step(Mode::Current)?;
    alice.step(Mode::Current)?;
    alice.enqueue(b"hello Bob".to_vec());
    bob.enqueue(b"hello Alice".to_vec());
    alice.step(Mode::Current)?;
    bob.step(Mode::Current)?;
    alice.step(Mode::Current)?;
    assert_eq!(alice.core().received(), [b"hello Alice".to_vec()]);
    assert_eq!(bob.core().received(), [b"hello Bob".to_vec()]);
    println!("Alice received: hello Alice");
    println!("Bob received: hello Bob");
    Ok(())
}
