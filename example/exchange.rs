use rendezqueue::{Client, Mode, Outcome, Server};

fn rpc(server: &mut Server, client: &mut Client, name: &str, mode: Mode) {
    let Some(packet) = client.send(mode).expect("counter capacity") else {
        return;
    };
    let request = &packet.request;
    let kind = if request.is_reference() {
        "reference"
    } else if request.ack.is_none() {
        "open"
    } else if request.values.is_empty() {
        "empty full"
    } else {
        "data full"
    };
    let outcome = server.serve(request);
    let status = match &outcome {
        Outcome::Reject => "rejected",
        Outcome::Ok(reply) if reply.nxt.is_some() => "completed",
        Outcome::Ok(reply) if reply.ack.is_some() => "peer found",
        Outcome::Ok(_) => "pending",
    };
    println!("{name}: {kind} -> {status}");
    client.accept(&packet, &outcome).expect("counter capacity");
}

fn main() {
    let mut server = Server::default();
    let mut alice = Client::new(b"example-alice".to_vec());
    let mut bob = Client::new(b"example-bob".to_vec());
    rpc(&mut server, &mut alice, "Alice", Mode::Current);
    rpc(&mut server, &mut bob, "Bob", Mode::Current);
    rpc(&mut server, &mut alice, "Alice", Mode::Current);
    alice.enqueue(b"hello Bob".to_vec());
    bob.enqueue(b"hello Alice".to_vec());
    rpc(&mut server, &mut alice, "Alice", Mode::Current);
    rpc(&mut server, &mut bob, "Bob", Mode::Current);
    println!("Simulate server cache loss.");
    server.reset();
    rpc(&mut server, &mut alice, "Alice", Mode::Current);
    rpc(&mut server, &mut alice, "Alice", Mode::Current);
    rpc(&mut server, &mut bob, "Bob", Mode::Previous);
    rpc(&mut server, &mut bob, "Bob", Mode::Previous);
    rpc(&mut server, &mut alice, "Alice", Mode::Current);
    assert_eq!(alice.received(), [b"hello Alice".to_vec()]);
    assert_eq!(bob.received(), [b"hello Bob".to_vec()]);
    for (name, client) in [("Alice", alice), ("Bob", bob)] {
        let text: Vec<_> = client
            .received()
            .iter()
            .map(|v| String::from_utf8_lossy(v))
            .collect();
        println!(
            "{name} received: {text:?}; generation {}",
            client.generation()
        );
    }
}
