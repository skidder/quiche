// Copyright (C) 2026, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

//! Reproduction of the tx_buffered bug fixed in commit aea75924.
//!
//! This example demonstrates the issue where `tx_buffered` (the counter tracking
//! bytes in the send buffer) was not updated when bytes were ACKed or lost on
//! streams that had already been closed by the client.
//!
//! ## The Bug
//!
//! When a client cancels a request (sends STOP_SENDING), the stream is removed
//! from the server's stream map. However, there may still be in-flight packets
//! containing data for that stream.
//!
//! Before the fix:
//! - When processing ACKs, the code first checked if the stream exists
//! - If the stream was closed, it would skip updating tx_buffered
//! - This caused tx_buffered to accumulate and never decrease
//! - Result: The server's send capacity would appear exhausted
//!
//! After the fix:
//! - tx_buffered is decremented BEFORE checking if the stream exists
//! - The counter stays accurate even for closed streams
//!
//! ## Symptoms in Production
//!
//! This bug caused HTTP/3 requests to stall for 60+ seconds in production
//! (e.g., video playback stalls on Discord) because the server thought its
//! send buffer was full when it actually wasn't.
//!
//! ## Running
//!
//! ```bash
//! cargo run --example tx_buffered_repro
//! ```

use quiche::test_utils::{emit_flight, process_flight, Pipe};
use quiche::{Config, Shutdown, TxBufferTrackingState, PROTOCOL_VERSION};

fn main() {
    // Run the reproduction with different congestion control algorithms
    for cc in &["cubic", "bbr2"] {
        println!("\n============================================================");
        println!("Testing with congestion control: {}", cc);
        println!("============================================================\n");

        if let Err(e) = run_reproduction(cc) {
            eprintln!("Error: {:?}", e);
        }
    }
}

fn run_reproduction(cc_algorithm: &str) -> quiche::Result<()> {
    // Configure the connection with generous limits
    let mut config = Config::new(PROTOCOL_VERSION)?;
    config.set_cc_algorithm_name(cc_algorithm)?;
    config.set_application_protos(&[b"h3"])?;
    config.load_cert_chain_from_pem_file("examples/cert.crt")?;
    config.load_priv_key_from_pem_file("examples/cert.key")?;
    config.set_initial_max_data(50000);
    config.set_initial_max_stream_data_bidi_local(120000);
    config.set_initial_max_stream_data_bidi_remote(120000);
    config.set_initial_max_streams_bidi(10);
    config.set_max_recv_udp_payload_size(1200);
    config.verify_peer(false);

    // Create a simulated client-server connection pair
    let mut pipe = Pipe::with_config(&mut config)?;
    pipe.handshake()?;

    println!("1. Connection established");

    // Get initial send capacity
    let initial_capacity = get_send_capacity(&mut pipe);
    println!("   Initial send capacity: {} bytes", initial_capacity);

    // Client initiates multiple streams (simulating HTTP/3 requests)
    pipe.client.stream_send(0, b"GET /video1", true)?;
    pipe.client.stream_send(4, b"GET /video2", false)?; // Keep open for response
    pipe.client.stream_send(8, b"GET /video3", true)?;
    pipe.advance()?;

    println!("2. Client sent requests on streams 0, 4, 8");

    // Server reads the requests
    let mut buf = [0; 1024];
    pipe.server.stream_recv(0, &mut buf)?;
    pipe.server.stream_recv(4, &mut buf)?;
    pipe.server.stream_recv(8, &mut buf)?;
    pipe.advance()?;

    println!("3. Server received requests");

    // Server sends a large response on stream 4 (larger than cwnd)
    // This simulates a video chunk being sent
    let large_response = vec![0u8; 50000];
    let sent = pipe.server.stream_send(4, &large_response, false)?;

    println!("4. Server queued {} bytes on stream 4", sent);

    // Get the current congestion window
    let cwnd = pipe
        .server
        .path_stats()
        .next()
        .map(|p| p.cwnd)
        .unwrap_or(0);
    println!("   Congestion window (cwnd): {}", cwnd);

    // Check capacity after queuing
    let capacity_after_queue = get_send_capacity(&mut pipe);
    println!("   Send capacity after queue: {} bytes", capacity_after_queue);

    // Emit the server's packets (puts bytes in flight)
    let server_flight = emit_flight(&mut pipe.server)?;
    println!(
        "5. Server emitted {} packets (bytes now in flight)",
        server_flight.len()
    );

    // **KEY SCENARIO**: Client cancels stream 4 (e.g., user seeks in video)
    // This sends STOP_SENDING to the server
    pipe.client.stream_shutdown(4, Shutdown::Read, 42)?;
    println!("6. Client cancelled stream 4 (sent STOP_SENDING)");

    let client_flight = emit_flight(&mut pipe.client)?;
    println!(
        "   Client emitted {} packets with STOP_SENDING",
        client_flight.len()
    );

    // Process the STOP_SENDING at the server (removes stream 4)
    process_flight(&mut pipe.server, client_flight)?;
    println!("7. Server received STOP_SENDING, stream 4 removed");

    // Process the server's original data packets at the client
    // Client will ACK these packets
    process_flight(&mut pipe.client, server_flight)?;
    println!("8. Client received server data, will send ACKs");

    // Let the connection advance (processes ACKs)
    pipe.advance()?;
    println!("9. Connection advanced (ACKs processed)");

    // **THE BUG**: Before the fix, tx_buffered would still be > 0 here
    // because ACKs for closed streams weren't updating the counter.
    // This would cause tx_buffered_state to be Inconsistent.
    let tx_state = pipe.server.stats().tx_buffered_state;

    println!("\n--- RESULTS ---");
    println!("tx_buffered_state: {:?}", tx_state);

    // Try to send on another stream to verify capacity
    let can_send = pipe.server.stream_send(8, &large_response, false)?;
    println!("Bytes sendable on stream 8: {}", can_send);

    // Get final send capacity
    let final_capacity = get_send_capacity(&mut pipe);
    println!("Final send capacity: {} bytes", final_capacity);

    // Verify the fix worked
    if tx_state == TxBufferTrackingState::Ok {
        println!("\n✓ SUCCESS: tx_buffered_state is Ok");
        println!("  The fix (aea75924) is working correctly.");
        println!("  Send capacity is properly restored after stream cancellation.");

        // Additional verification: send capacity should be approximately equal to cwnd
        // (since the buffer should be empty after ACKs are processed)
        if can_send > 0 {
            println!("  Server can send {} bytes on stream 8.", can_send);
        }
    } else {
        println!("\n✗ FAILURE: tx_buffered_state is {:?}", tx_state);
        println!("  This indicates the bug is present.");
        println!("  The server incorrectly thinks its send buffer is still full.");
        println!(
            "  Send capacity is only {} bytes (should be near cwnd: {}).",
            can_send, cwnd
        );
    }

    Ok(())
}

/// Helper to estimate send capacity by trying to send on a test stream
fn get_send_capacity(pipe: &mut Pipe) -> usize {
    // This is an approximation - we try to send a large buffer and see how much is accepted
    let test_buf = vec![0u8; 100000];

    // Use stream 12 as a test stream (different from the main test streams)
    match pipe.server.stream_send(12, &test_buf, false) {
        Ok(n) => n,
        Err(_) => 0,
    }
}
