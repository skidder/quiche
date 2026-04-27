//! Standalone repro for tokio-quiche STOP_SENDING deadlock
//!
//! Demonstrates the WaitForDownstreamData hang without the tokio-quiche test harness.
//! Models the exact channel ownership state machine from the driver.
//!
//! Run: rustc --edition 2021 standalone-repro.rs && ./standalone-repro
//! Or:  Create a Cargo project with `tokio = { version = "1", features = ["full"] }`
//!      and replace main with the code below.

use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

// ─── Types mirroring tokio-quiche internals ─────────────────────────────────

type OutboundFrame = &'static str; // simplified

struct StreamCtx {
    // OutboundFrameStream receiver — taken by wait_for_recv() into WaitForDownstreamData
    recv: Option<mpsc::Receiver<OutboundFrame>>,
    fin_or_reset_sent: bool,
    queued_frame: Option<OutboundFrame>,
}

struct WaitForDownstreamData {
    stream_id: u64,
    chan: Option<mpsc::Receiver<OutboundFrame>>,
}

impl StreamCtx {
    fn new() -> (Self, mpsc::Sender<OutboundFrame>) {
        let (tx, rx) = mpsc::channel(16);
        (
            Self { recv: Some(rx), fin_or_reset_sent: false, queued_frame: None },
            tx,
        )
    }

    // Mirrors tokio-quiche/src/http3/driver/streams.rs:104
    fn wait_for_recv(&mut self, stream_id: u64) -> WaitForDownstreamData {
        WaitForDownstreamData {
            stream_id,
            chan: self.recv.take(), // ← ctx.recv is now None
        }
    }

    // Mirrors tokio-quiche/src/http3/driver/streams.rs:116
    // POST PR #2420 and #2438 — early-return guards added, but waiting_streams not notified
    fn handle_recvd_stop_sending(&mut self, wire_err_code: u64) {
        println!("[Driver] handle_recvd_stop_sending({})", wire_err_code);

        if self.fin_or_reset_sent {
            println!("[Driver] Early return: fin_or_reset_sent already true");
            return;
        }

        self.fin_or_reset_sent = true;
        self.queued_frame = None;
        self.recv = None; // ← NO-OP: already None from wait_for_recv()
        println!("[Driver] Set self.recv = None (was already None — NO-OP)");
        println!("[Driver] waiting_streams NOT iterated — WaitForDownstreamData not notified");
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    println!("=== tokio-quiche STOP_SENDING deadlock repro ===\n");
    println!("Scenario: server in wait_for_recv() when client sends STOP_SENDING\n");

    let (mut ctx, outbound_tx) = StreamCtx::new();
    let stream_id = 0u64;

    // Step 1: Empty outbound channel → wait_for_recv() → WaitForDownstreamData
    // This is what process_writable_stream does when the app has no frames to send yet
    let waiting = ctx.wait_for_recv(stream_id);
    println!("[Driver] Stream {} entered waiting_streams via wait_for_recv()", stream_id);
    println!("[Driver] ctx.recv = None: {}", ctx.recv.is_none());
    println!("[Driver] WaitForDownstreamData holds channel: {}", waiting.chan.is_some());

    // Step 2: Client sends STOP_SENDING
    println!("\n[Client] Sending STOP_SENDING...");
    ctx.handle_recvd_stop_sending(4242);

    println!("[Driver] OutboundFrameSender still open: {}", !outbound_tx.is_closed());
    println!("[Driver] WaitForDownstreamData still holds channel: {}", waiting.chan.is_some());

    // Step 3: Demonstrate the hang
    println!("\n[Driver] Polling WaitForDownstreamData (with 5s timeout)...");
    let mut rx = waiting.chan.unwrap();
    let result = timeout(
        Duration::from_secs(5),
        async move { rx.recv().await }
    ).await;

    println!();
    match result {
        Err(_) => {
            println!("*** BUG CONFIRMED: WaitForDownstreamData HUNG for 5 seconds ***");
            println!("    In production: stream leaks in waiting_streams until connection");
            println!("    closes (max_idle_timeout, default 30s).");
        }
        Ok(_) => println!("Channel closed (fix applied)"),
    }

    println!("\n--- Root cause ---");
    println!("handle_recvd_stop_sending(): self.recv = None (ctx.recv was already None)");
    println!("wait_for_recv() had already moved recv into WaitForDownstreamData.");
    println!("The assignment is a no-op — the channel remains open.");
    println!();
    println!("--- Fix ---");
    println!("In process_writable_stream() StreamStopped arm, after handle_recvd_stop_sending():");
    println!("  if ctx.recv.is_none() {{");
    println!("      for pending in self.waiting_streams.iter_mut() {{");
    println!("          match pending {{");
    println!("              WaitForStream::Downstream(WaitForDownstreamData {{");
    println!("                  stream_id: id, chan: Some(chan),");
    println!("              }}) if *id == stream_id => {{ chan.close(); break; }},");
    println!("              _ => {{}},");
    println!("          }}");
    println!("      }}");
    println!("  }}");
    println!("(Mirrors the existing h3::Event::Reset handler at driver/mod.rs:656)");
}
