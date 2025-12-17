use crate::http3::driver::client::ClientHooks;
use crate::http3::driver::server::ServerHooks;
use assert_matches::assert_matches;

use super::test_utils::*;
use super::*;

/// Tests that use an H3Driver for the client side. We mostly focus on testing
/// the driver's handling of stream state, and data, rather than H3 semantics.
/// Note that most of these tests could have just as easily been written for
/// the server side.
mod client_side_driver {
    use super::*;

    #[test]
    fn client_fin_before_server_body() {
        let mut helper = DriverTestHelper::<ClientHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a request
        let stream_id = helper
            .driver_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_server_poll().unwrap(),
            (0, h3::Event::Headers { .. })
        );
        helper.peer_server_send_response(0, false).unwrap();

        helper.advance_and_run_loop().unwrap();

        // Client receives response headers
        let resp = assert_matches!(
            helper.driver_recv_core_event().unwrap(),
            H3Event::IncomingHeaders(headers) => { headers }
        );
        assert_eq!(resp.stream_id, stream_id);
        assert!(!resp.read_fin);
        let to_server = resp.send.get_ref().unwrap().clone();
        let mut from_server = resp.recv;
        // client sends body
        to_server
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[1; 5]),
                false,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // server receives client body
        assert_eq!(helper.peer_server_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(helper.peer_server_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_server_recv_body_vec(0, 1024), Ok(vec![1; 5]));

        // client sends fin, server sends body and fin
        to_server
            .try_send(OutboundFrame::Body(BufFactory::get_empty_buf(), true))
            .unwrap();
        helper.peer_server_send_body(0, &[2; 10], true).unwrap();

        // Server reads fin
        helper.advance_and_run_loop().unwrap();
        // TODO: the server sees an h3::Event::Data, but it's for an empty buffer.
        // Ideally, it wouldn't do that.
        assert_eq!(helper.peer_server_poll(), Ok((0, h3::Event::Data)));
        // No data to be read
        assert_eq!(
            helper.peer_server_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );
        assert_eq!(helper.peer_server_poll(), Ok((0, h3::Event::Finished)));
        assert_eq!(helper.peer_server_poll(), Err(h3::Error::Done));
        helper.advance_and_run_loop().unwrap();

        // client receives the server body
        assert_matches!(from_server.try_recv(), Ok(InboundFrame::Body(buf, fin)) => {
            assert_eq!(buf.into_inner().into_vec(), vec![2; 10]);
            // TODO: it would be nice if we could receive the fin here, but that's not
            // how quiche::h3 works. Instead we need another receive call on the channel
            assert!(!fin);
        });
        helper.work_loop_iter().unwrap();

        // FIXME: This is an edge case. We should not see a `Disconnected` error
        // here. The `from_server` / `InboudFrame` channel is set to 1 in tests.
        // What happens, is the driver reads the previous body frame, then it
        // sees an `Event::Finished` and calls `process_h3_fin`, which sets
        // `ctx.fin_recv`. Then it processes the pending write that sends the fin
        // from client to server. The driver now sees both ctx.fin_read &&
        // ctx.fin_sent and drops the context and thus the channel. Application
        // code (H3Body) is not affected by -- it treats a disconnected channel
        // like receiving a fin. It's a different question if it should treat it
        // as such

        // assert_matches!(from_server.try_recv(), Ok(InboundFrame::Body(buf,
        // fin)) => {
        //    assert_eq!(buf.into_inner().into_vec().len(), 0);
        //    assert!(fin);
        //});
        assert_matches!(from_server.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(helper.driver.stream_map.len(), 0);
    }
    /// Test that dropping the OutboundFrame channel causes the driver to
    /// send a RESET_STREAM frame to the peer.
    #[test]
    fn client_send_reset_stream_when_outbound_frame_channel_drops() {
        let mut helper = DriverTestHelper::<ClientHooks>::new().unwrap();
        const REQUEST_CANCELED_ERR: u64 =
            h3::WireErrorCode::RequestCancelled as u64;
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // The client uses H3Driver
        // client sends a request
        let stream_id = helper
            .driver_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_server_poll().unwrap(),
            (0, h3::Event::Headers { .. })
        );
        helper.peer_server_send_response(0, false).unwrap();

        helper.advance_and_run_loop().unwrap();

        // Client receives response headers
        let resp = assert_matches!(
            helper.driver_recv_core_event().unwrap(),
            H3Event::IncomingHeaders(headers) => { headers }
        );
        assert_eq!(resp.stream_id, stream_id);
        assert!(!resp.read_fin);
        // the stream is waiting on writes
        assert_eq!(helper.driver.waiting_streams.len(), 1);
        // take the InboundFrame receiver and stats
        let mut from_server = resp.recv;
        let audit_stats = resp.h3_audit_stats.clone();
        // ... and drop the outbound frame
        drop(resp.send);

        helper.advance_and_run_loop().unwrap();

        // server receives the reset
        assert_eq!(
            helper.peer_server_poll(),
            Ok((0, h3::Event::Reset(REQUEST_CANCELED_ERR)))
        );
        assert_eq!(helper.peer_server_poll(), Err(h3::Error::Done));

        helper.peer_server_send_body(0, &[2; 10], true).unwrap();
        helper.advance_and_run_loop().unwrap();

        // client receives the server body
        assert_matches!(from_server.try_recv(), Ok(InboundFrame::Body(buf, fin)) => {
            assert_eq!(buf.into_inner().into_vec(), vec![2; 10]);
            // TODO: it would be nice if we could receive the fin here, but that's not
            // how quiche::h3 works. Instead we need another receive call on the channel
            assert!(!fin);
        });
        helper.work_loop_iter().unwrap();
        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), -1);
        assert_eq!(
            audit_stats.sent_reset_stream_error_code(),
            REQUEST_CANCELED_ERR as i64
        );
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 10);
        assert_eq!(audit_stats.downstream_bytes_sent(), 0);
    }

    /// Test that dropping the OutboundFrame channel causes the driver to
    /// send a RESET_STREAM frame to the peer.
    #[test]
    fn client_send_reset_stream_when_outbound_frame_channel_drops_2() {
        let mut helper = DriverTestHelper::<ClientHooks>::new().unwrap();
        const REQUEST_CANCELED_ERR: u64 =
            h3::WireErrorCode::RequestCancelled as u64;
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // The client uses H3Driver
        // client sends a request
        let stream_id = helper
            .driver_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers, body, and fin
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_server_poll().unwrap(),
            (0, h3::Event::Headers { .. })
        );
        helper.peer_server_send_response(0, false).unwrap();
        helper.peer_server_send_body(0, &[2; 10], true).unwrap();

        helper.advance_and_run_loop().unwrap();

        // Client receives response headers
        let mut resp = assert_matches!(
            helper.driver_recv_core_event().unwrap(),
            H3Event::IncomingHeaders(headers) => { headers }
        );
        assert_eq!(resp.stream_id, stream_id);
        assert!(!resp.read_fin);
        // take the InboundFrame receiver and stats
        let mut from_server = resp.recv;
        let audit_stats = resp.h3_audit_stats.clone();
        let (body, fin, _) = helper.driver_try_recv_body(&mut from_server);
        assert_eq!(body, vec![2; 10]);
        assert!(fin);
        helper.advance_and_run_loop().unwrap();

        // clsoe the channel.
        resp.send.close();

        helper.advance_and_run_loop().unwrap();

        // server receives the reset
        assert_eq!(
            helper.peer_server_poll(),
            Ok((0, h3::Event::Reset(REQUEST_CANCELED_ERR)))
        );
        assert_eq!(helper.peer_server_poll(), Err(h3::Error::Done));

        helper.advance_and_run_loop().unwrap();

        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), -1);
        assert_eq!(
            audit_stats.sent_reset_stream_error_code(),
            REQUEST_CANCELED_ERR as i64
        );
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 10);
        assert_eq!(audit_stats.downstream_bytes_sent(), 0);
    }

    /// Send data until the stream is no longer writable, then drop the
    /// OutboundFrame channel to trigger a RESET_STREAM
    #[test]
    fn client_send_reset_stream_with_full_stream() {
        let mut config = default_quiche_config();
        config.set_initial_max_stream_data_bidi_local(30);
        config.set_initial_max_stream_data_bidi_remote(30);
        let mut helper = DriverTestHelper::<ClientHooks>::with_pipe(
            quiche::test_utils::Pipe::with_config(&mut config).unwrap(),
        )
        .unwrap();
        const REQUEST_CANCELED_ERR: u64 =
            h3::WireErrorCode::RequestCancelled as u64;
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // The client uses H3Driver
        // client sends a request
        let stream_id = helper
            .driver_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers, and fin
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_server_poll().unwrap(),
            (0, h3::Event::Headers { .. })
        );
        helper.peer_server_send_response(0, true).unwrap();

        helper.advance_and_run_loop().unwrap();

        // Client receives response headers
        let resp = assert_matches!(
            helper.driver_recv_core_event().unwrap(),
            H3Event::IncomingHeaders(headers) => { headers }
        );
        assert_eq!(resp.stream_id, stream_id);
        assert!(resp.read_fin);
        let audit_stats = resp.h3_audit_stats.clone();
        // send a body the to server, but not enough flow control for the full
        // body
        resp.send
            .get_ref()
            .unwrap()
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[23; 50]),
                false,
            ))
            .unwrap();
        assert_eq!(helper.driver.waiting_streams.len(), 1);
        // run `work_loop_iter()` to write the body into quiche
        helper.work_loop_iter().unwrap();
        // make sure we couldn't write the full body
        assert!(audit_stats.downstream_bytes_sent() < 50);
        let written = audit_stats.downstream_bytes_sent();
        // advance the pipe, the stream is writable again, but
        // don't advance the work_loop yet.
        helper.pipe.advance().unwrap();
        while helper.peer_server_poll().is_ok() {}
        assert_eq!(
            helper.peer_server_recv_body_vec(0, 1024).unwrap().len(),
            written as usize
        );
        helper.pipe.advance().unwrap();
        assert_eq!(helper.driver.waiting_streams.len(), 0);
        assert!(helper.driver.stream_map.get(&0).unwrap().recv.is_some());
        assert!(helper
            .driver
            .stream_map
            .get(&0)
            .unwrap()
            .queued_frame
            .is_some());

        // clsoe the channel.
        drop(resp.send);

        helper.work_loop_iter().unwrap();
        assert_eq!(
            audit_stats.sent_reset_stream_error_code(),
            REQUEST_CANCELED_ERR as i64
        );
        helper.advance_and_run_loop().unwrap();

        // server receives the reset
        assert_eq!(
            helper.peer_server_poll(),
            Ok((0, h3::Event::Reset(REQUEST_CANCELED_ERR)))
        );
        assert_eq!(helper.peer_server_poll(), Err(h3::Error::Done));

        helper.advance_and_run_loop().unwrap();

        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), -1);
        assert_eq!(
            audit_stats.sent_reset_stream_error_code(),
            REQUEST_CANCELED_ERR as i64
        );
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::None);
    }

    /// Test that dropping the OutboundFrame channel after we've send a fin
    /// is a no-op.
    #[test]
    fn client_drop_outbound_frame_channel_after_fin_no_reset() {
        let mut helper = DriverTestHelper::<ClientHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // The client uses H3Driver
        // client sends a request
        let stream_id = helper
            .driver_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers, body, and fin
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_server_poll().unwrap(),
            (0, h3::Event::Headers { .. })
        );
        helper.peer_server_send_response(0, false).unwrap();

        helper.advance_and_run_loop().unwrap();

        // Client receives response headers
        let mut resp = assert_matches!(
            helper.driver_recv_core_event().unwrap(),
            H3Event::IncomingHeaders(headers) => { headers }
        );
        assert_eq!(resp.stream_id, stream_id);
        assert!(!resp.read_fin);
        // take the InboundFrame receiver and stats
        let mut from_server = resp.recv;
        let audit_stats = resp.h3_audit_stats.clone();
        helper.advance_and_run_loop().unwrap();
        resp.send
            .get_ref()
            .unwrap()
            .try_send(OutboundFrame::Body(BufFactory::get_empty_buf(), true))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // clsoe the channel.
        resp.send.close();

        helper.advance_and_run_loop().unwrap();
        assert_eq!(helper.peer_server_send_body(0, &[42], true), Ok(1));
        helper.advance_and_run_loop().unwrap();

        // server receives the fin
        assert_eq!(helper.peer_server_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(
            helper.peer_server_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );
        assert_eq!(helper.peer_server_poll(), Ok((0, h3::Event::Finished)));
        assert_eq!(helper.peer_server_poll(), Err(h3::Error::Done));

        helper.advance_and_run_loop().unwrap();

        // client receives the body and fin
        let (body, fin, _err) = helper.driver_try_recv_body(&mut from_server);
        assert_eq!(body, &[42]);
        assert!(fin);

        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 1);
        assert_eq!(audit_stats.downstream_bytes_sent(), 0);
    }
}

/// Tests that use an H3Driver for the server side. We mostly focus on testing
/// the driver's handling of stream state, and data, rather than H3 semantics.
/// Note that most of these tests could have just as easily been written for
/// the client side.
mod server_side_driver {
    use super::*;

    #[test]
    fn client_fin_before_server_body() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let mut from_client = req.recv;
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();

        // client reads response and sends body and fin
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_send_body(0, &[1; 5], true), Ok(5));
        helper.advance_and_run_loop().unwrap();

        // server receives body
        let (body, fin, _err) = helper.driver_try_recv_body(&mut from_client);
        assert_eq!(body, vec![1; 5]);
        assert!(fin);

        // server sends body and fin
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[42]),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_recv_body_vec(0, 1024), Ok(vec![42]));
        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Finished)));

        assert_eq!(helper.driver.stream_map.len(), 0);
    }

    // This test verifies https://github.com/cloudflare/quiche/pull/2162
    #[test]
    fn verify_pr_2162() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a request but NO FIN.
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let mut from_client = req.recv;
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.work_loop_iter().unwrap();
        // server sends body and fin. This caused an infinite loop before #2162
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[42]),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends body and fin
        helper.advance_and_run_loop().unwrap();
        assert_eq!(helper.peer_client_send_body(0, &[1; 5], true), Ok(5));
        helper.advance_and_run_loop().unwrap();

        let (body, fin, _err) = helper.driver_try_recv_body(&mut from_client);
        assert_eq!(body, &[1; 5]);
        assert!(fin);

        // Stream is done
        assert_eq!(helper.driver.stream_map.len(), 0);
    }

    /// Test the case where the client sends a STOP_SENDING quiche frame.
    #[test]
    fn client_sends_stop_sending() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let mut from_client = req.recv;
        let audit_stats = req.h3_audit_stats;

        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();

        // client sends a STOP_SENDING
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Read, 4242),
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // the client didn't send any additional data, a try_recv on the server
        // returns empty
        assert_matches!(from_client.try_recv(), Err(TryRecvError::Empty));
        // The way quiche is implemented, we need to attempt a write to the stream
        // to learn that it's closed. So we add an OutboundFrame to the
        // channel and let the driver write it. The driver gets a
        // StreamStopped back and closes the channel.
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[23; 10]),
                false,
            ))
            .unwrap();
        helper.work_loop_iter().unwrap();
        assert!(to_client.is_closed());
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), 4242);
        helper.work_loop_iter().unwrap();

        // STOP_SENDING only closes one half of the stream. The client
        // can still send data and it MUST send a `fin` to close the
        // other half.
        helper.peer_client_send_body(0, &[1, 2, 3], true).unwrap();
        helper.advance_and_run_loop().unwrap();
        let (body, fin, _err) = helper.driver_try_recv_body(&mut from_client);
        assert_eq!(body, &[1, 2, 3]);
        assert!(fin);

        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), 4242);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        // technically quiche will automatically respond to a STOP_SENDING
        // frame with a STREAM_RESET echoing the error code, but the user
        // didn't *actively* send a STREAM_RESET.
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 3);
        assert_eq!(audit_stats.downstream_bytes_sent(), 0);
    }

    /// Test the case where the client sends a RESET_STREAM quiche frame.
    /// The peer sends its reset before we send a fin
    #[test]
    fn client_sends_reset_stream_before_server_fin() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client (peer) sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let from_client = req.recv;
        let audit_stats = req.h3_audit_stats;

        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();

        // client sends a RESET_STREAM frame
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 4242),
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // The channel is closed because the peer send us the reset.
        assert!(from_client.is_closed());
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), 4242);
        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::ResetStream { stream_id: 0 })
        );

        // We can still write to the peer and in fact, we must eventually send a
        // fin.
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[5; 4]),
                false,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[6; 4]),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Ok(vec![5, 5, 5, 5, 6, 6, 6, 6])
        );

        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), 4242);
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 0);
        assert_eq!(audit_stats.downstream_bytes_sent(), 8);
    }

    /// Test the case where the client sends a RESET_STREAM quiche frame.
    /// We send a fin before the client sends reset
    #[test]
    fn client_sends_reset_stream_after_server_fin() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client (peer) sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let from_client = req.recv;
        let audit_stats = req.h3_audit_stats;

        // Send response, body, and fin to client
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.work_loop_iter().unwrap();
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(b"foobar 42"),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a RESET_STREAM frame
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_matches!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        helper.peer_client_recv_body_vec(0, 1024).unwrap();
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 4242),
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // The channel is closed because the peer send us the reset.
        assert!(from_client.is_closed());
        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::ResetStream { stream_id: 0 })
        );

        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), 4242);
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 0);
        assert_eq!(
            audit_stats.downstream_bytes_sent(),
            b"foobar 42".len() as u64
        );
    }

    /// Test the case where the client sends a RESET_STREAM quiche frame while
    /// we're in the middle of reading data. We want to excercise the
    /// code-path where `upstream_ready` is called before `process_reads`.
    /// If `process_reads()` is called first, it will get the Reset event.
    /// If `upstream_ready()` is called first, it will attempt to read from
    /// the h3::Connection and will get a
    /// `TransportError(StreamReset(code))`
    #[test]
    fn client_sends_reset_stream_while_reading_wait_for_data() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client (peer) sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers and some body bytes
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let mut from_client = req.recv;
        let audit_stats = req.h3_audit_stats;

        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.work_loop_iter().unwrap();
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[1, 2, 3, 4]),
                false,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends data
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_matches!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_send_body(0, &[1; 10], false), Ok(10));

        // Advance the pipe and let the driver read a part of the body and
        // put it into the `from_client` channel
        helper.pipe.advance().unwrap();
        // Limit the amount of data we read from the stream.
        helper.driver.pooled_buf = BufFactory::buf_from_slice(&[0; 5]);
        helper.work_loop_iter().unwrap();
        assert_matches!(from_client.try_recv(), Ok(InboundFrame::Body(buf, fin)) => {
            assert_eq!(buf.into_inner().into_vec(), &[1; 5]);
            assert!(!fin);
        });
        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::BodyBytesReceived {
                stream_id: 0,
                num_bytes: 5,
                fin: false
            })
        );
        assert_matches!(
            helper.controller.event_receiver_mut().try_recv(),
            Err(TryRecvError::Empty)
        );

        // client sends a reset.
        // TODO: This is a bit finnicky to test properly. We don't want to
        // run a full `work_loop_iter()` because that would call `process_reads()`
        // first.
        helper.pipe.advance().unwrap();
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 4242),
            Ok(())
        );
        helper.pipe.advance().unwrap();
        tokio::task::unconstrained(
            helper.driver.wait_for_data(&mut helper.pipe.server),
        )
        .now_or_never()
        .unwrap_or(Ok(()))
        .unwrap();

        // The channel is closed because the peer send us the reset.
        assert!(from_client.is_closed());
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), 4242);
        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::ResetStream { stream_id: 0 })
        );

        // We can still write to the peer and in fact, we must eventually send a
        // fin.
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[6; 4]),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Ok(vec![1, 2, 3, 4, 6, 6, 6, 6])
        );
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Finished)));

        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), 4242);
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 5);
        assert_eq!(audit_stats.downstream_bytes_sent(), 8);
    }

    /// Test the case where the client sends a RESET_STREAM quiche frame while
    /// we're in the middle of reading data. We want to excercise the
    /// code-path where where we call `process_reads` before
    /// `upstream_ready()`.
    #[test]
    fn server_sends_reset_stream_while_reading_process_reads() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client (peer) sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let mut from_client = req.recv;
        let audit_stats = req.h3_audit_stats;

        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends data
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_send_body(0, &[1; 10], false), Ok(10));

        // Advance the pipe and let the driver read a part of the body and
        // put it into the `from_client` channel
        helper.pipe.advance().unwrap();
        // Limit the amount of data we read from the stream.
        helper.driver.pooled_buf = BufFactory::buf_from_slice(&[0; 5]);
        helper.work_loop_iter().unwrap();
        assert_matches!(from_client.try_recv(), Ok(InboundFrame::Body(buf, fin)) => {
            assert_eq!(buf.into_inner().into_vec(), &[1; 5]);
            assert!(!fin);
        });
        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::BodyBytesReceived {
                stream_id: 0,
                num_bytes: 5,
                fin: false
            })
        );

        // client sends a reset.
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 4242),
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // The channel is closed because the peer send us the reset.
        assert!(from_client.is_closed());
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), 4242);
        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::ResetStream { stream_id: 0 })
        );

        // send fin to client
        to_client
            .try_send(OutboundFrame::Body(BufFactory::get_empty_buf(), true))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Finished)));

        assert_eq!(helper.driver.stream_map.len(), 0);
        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), 4242);
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 5);
        assert_eq!(audit_stats.downstream_bytes_sent(), 0);
    }

    #[test]
    fn server_driver_send_stop_sending_after_channel_drop() {
        const REQUEST_CANCELED_ERR: u64 =
            h3::WireErrorCode::RequestCancelled as u64;
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        let audit_stats = req.h3_audit_stats.clone();
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let mut from_client = req.recv;
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();

        // client reads response and sends body without fin
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_send_body(0, &[1; 5], false), Ok(5));
        helper.advance_and_run_loop().unwrap();

        // server receives body
        let (body, fin, _err) = helper.driver_try_recv_body(&mut from_client);
        assert_eq!(body, vec![1; 5]);
        assert!(!fin);

        // peer (client) sends more data
        assert_eq!(helper.peer_client_send_body(0, &[1; 6], false), Ok(6));
        // advance the pipe only
        helper.pipe.advance().unwrap();
        // we drop the channel.
        drop(from_client);
        helper.advance_and_run_loop().unwrap();

        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::BodyBytesReceived {
                stream_id: 0,
                num_bytes: 5,
                fin: false
            })
        );
        assert_matches!(
            helper.controller.event_receiver_mut().try_recv(),
            Err(TryRecvError::Empty)
        );

        // Make sure the peer has received our STOP_SENDING frame
        assert_eq!(
            helper.peer_client_send_body(0, &[1; 7], false),
            Err(h3::Error::TransportError(quiche::Error::StreamStopped(
                REQUEST_CANCELED_ERR
            )))
        );
        helper.advance_and_run_loop().unwrap();

        // we still need to send a fin
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[42]),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_recv_body_vec(0, 1024), Ok(vec![42]));
        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Finished)));

        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(
            audit_stats.sent_stop_sending_error_code(),
            REQUEST_CANCELED_ERR as i64
        );
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 5);
        assert_eq!(audit_stats.downstream_bytes_sent(), 1);
        assert_eq!(helper.driver.stream_map.len(), 0);
    }

    // Verify we don't send a STOP_SENDING frame if we've already processed a
    // fin
    #[test]
    fn server_driver_drop_channel_after_fin() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        let audit_stats = req.h3_audit_stats.clone();
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let mut from_client = req.recv;
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();

        // client reads response and sends body WITH fin
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_send_body(0, &[1; 5], true), Ok(5));
        helper.advance_and_run_loop().unwrap();

        // server receives body
        let (body, fin, _err) = helper.driver_try_recv_body(&mut from_client);
        assert_eq!(body, vec![1; 5]);
        assert!(fin);

        helper.advance_and_run_loop().unwrap();
        // we drop the channel.
        drop(from_client);
        helper.advance_and_run_loop().unwrap();

        // we still need to send a fin
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[42]),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_recv_body_vec(0, 1024), Ok(vec![42]));
        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Finished)));

        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 5);
        assert_eq!(audit_stats.downstream_bytes_sent(), 1);
        assert_eq!(helper.driver.stream_map.len(), 0);
    }

    // Test the edge case where the driver has read a fin from the stream but
    // hasn't been able to deliver it before the channel is dropped.
    #[test]
    fn server_driver_drop_channel_after_fin_2() {
        const REQUEST_CANCELED_ERR: u64 =
            h3::WireErrorCode::RequestCancelled as u64;
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        let audit_stats = req.h3_audit_stats.clone();
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();

        // client reads response and sends body without fin
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_send_body(0, &[1; 5], false), Ok(5));
        helper.advance_and_run_loop().unwrap();

        // peer (client) sends more data and fin
        assert_eq!(helper.peer_client_send_body(0, &[1; 6], true), Ok(6));
        helper.advance_and_run_loop().unwrap();
        // we drop the channel.
        drop(req.recv);
        helper.advance_and_run_loop().unwrap();

        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::BodyBytesReceived {
                stream_id: 0,
                num_bytes: 5,
                fin: false
            })
        );
        assert_matches!(
            helper.controller.event_receiver_mut().try_recv(),
            Err(TryRecvError::Empty)
        );

        // we still need to send a fin
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[42]),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_recv_body_vec(0, 1024), Ok(vec![42]));
        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Finished)));

        assert_eq!(audit_stats.recvd_stop_sending_error_code(), -1);
        assert_eq!(audit_stats.recvd_reset_stream_error_code(), -1);
        assert_eq!(audit_stats.sent_reset_stream_error_code(), -1);
        assert_eq!(
            audit_stats.sent_stop_sending_error_code(),
            REQUEST_CANCELED_ERR as i64
        );
        assert_eq!(audit_stats.recvd_stream_fin(), StreamClosureKind::None);
        assert_eq!(audit_stats.sent_stream_fin(), StreamClosureKind::Explicit);
        assert_eq!(audit_stats.downstream_bytes_recvd(), 5);
        assert_eq!(audit_stats.downstream_bytes_sent(), 1);
        assert_eq!(helper.driver.stream_map.len(), 0);
    }

    #[test]
    fn server_send_trailers() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // client sends a request
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();

        // servers reads request and sends response headers
        helper.advance_and_run_loop().unwrap();
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, stream_id);
        assert!(!req.read_fin);
        let to_client = req.send.get_ref().unwrap().clone();
        let mut from_client = req.recv;
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();

        // client reads response and sends body and fin
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
        assert_eq!(helper.peer_client_send_body(0, &[1; 5], true), Ok(5));
        helper.advance_and_run_loop().unwrap();

        // server receives body
        let (body, fin, _err) = helper.driver_try_recv_body(&mut from_client);
        assert_eq!(body, vec![1; 5]);
        assert!(fin);

        // server sends body
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[42]),
                false,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        assert_eq!(helper.peer_client_recv_body_vec(0, 1024), Ok(vec![42]));
        assert_eq!(
            helper.peer_client_recv_body_vec(0, 1024),
            Err(h3::Error::Done)
        );

        // server sends trailers
        to_client
            .try_send(OutboundFrame::Trailers(make_response_trailers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );

        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Finished)));
        assert_eq!(helper.peer_client_poll(), Err(h3::Error::Done));
    }

    /// Test scenario: Client cancels stream 0 while reading body, then immediately
    /// sends a new request on stream 4 (like a video seek operation).
    /// This tests that the new stream receives its body data promptly without
    /// being blocked by the cancelled stream.
    ///
    /// Bug context: HTTP/3 range requests were experiencing 60-second delays where
    /// headers arrived immediately but body was delayed. This occurred after
    /// cancelling a previous stream (RST_STREAM) and starting a new range request.
    #[test]
    fn new_stream_after_client_cancels_previous_stream() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // === Stream 0: Initial request that will be cancelled ===
        let stream_id_0 = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();
        assert_eq!(stream_id_0, 0);

        helper.advance_and_run_loop().unwrap();
        let req0 = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req0.stream_id, 0);
        let to_client_0 = req0.send.get_ref().unwrap().clone();

        // Server sends response headers for stream 0
        to_client_0
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives headers on stream 0
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );

        // Server starts sending body data on stream 0
        to_client_0
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[1; 50]),
                false,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives some data on stream 0
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        let _ = helper.peer_client_recv_body_vec(0, 1024);

        // === Client cancels stream 0 (like a video seek) ===
        // Client sends RST_STREAM on stream 0
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 268), // H3_REQUEST_CANCELLED
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // Server should see the reset event
        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::ResetStream { stream_id: 0 })
        );

        // === Stream 4: New request (like range request for end of file) ===
        let stream_id_4 = helper
            .peer_client_send_request(make_request_headers("GET"), true) // fin=true for simple request
            .unwrap();
        assert_eq!(stream_id_4, 4); // Next client-initiated bidi stream

        helper.advance_and_run_loop().unwrap();

        // Server receives the new request on stream 4
        let req4 = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req4.stream_id, 4);
        let to_client_4 = req4.send.get_ref().unwrap().clone();
        let mut from_client_4 = req4.recv;

        // Server sends response headers on stream 4
        to_client_4
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives headers on stream 4
        assert_matches!(
            helper.peer_client_poll(),
            Ok((4, h3::Event::Headers { .. }))
        );

        // Server sends body data on stream 4 - THIS SHOULD NOT BE DELAYED
        to_client_4
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[42; 100]),
                true, // fin
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client should receive body data on stream 4 immediately
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Data)));
        let body = helper.peer_client_recv_body_vec(4, 1024).unwrap();
        assert_eq!(body, vec![42; 100]);

        // Stream 4 should be finished
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Finished)));

        // Verify stream 0 is cleaned up and stream 4 worked correctly
        assert!(!helper.driver.stream_map.contains_key(&0) ||
                helper.driver.stream_map.get(&0).unwrap().fin_or_reset_recv);

        // Verify waiting_streams doesn't contain stale futures for stream 0
        // that could block processing
        let (body4, fin4, _) = helper.driver_try_recv_body(&mut from_client_4);
        assert!(body4.is_empty() || fin4); // Should have received fin already
    }

    /// Test that multiple stream cancellations don't cause accumulation of
    /// stale futures in waiting_streams that could block new streams.
    #[test]
    fn multiple_stream_cancellations_dont_block_new_streams() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // Create and cancel multiple streams in sequence
        for i in 0..3u64 {
            let stream_id = i * 4; // Client-initiated bidi streams: 0, 4, 8

            // Client sends request
            let created_stream_id = helper
                .peer_client_send_request(make_request_headers("GET"), false)
                .unwrap();
            assert_eq!(created_stream_id, stream_id);

            helper.advance_and_run_loop().unwrap();

            // Server receives request
            let req = assert_matches!(
                helper.driver_recv_server_event().unwrap(),
                ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
            );
            assert_eq!(req.stream_id, stream_id);

            // Server sends response headers
            let to_client = req.send.get_ref().unwrap().clone();
            to_client
                .try_send(OutboundFrame::Headers(make_response_headers(), None))
                .unwrap();
            helper.advance_and_run_loop().unwrap();

            // Client receives headers - drain any events for other streams first
            loop {
                match helper.peer_client_poll() {
                    Ok((sid, h3::Event::Headers { .. })) if sid == stream_id => break,
                    Ok(_) => continue, // Drain other events
                    Err(h3::Error::Done) => panic!("Expected headers for stream {}", stream_id),
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            }

            // Client cancels the stream (simulating seek)
            assert_eq!(
                helper
                    .pipe
                    .client
                    .stream_shutdown(stream_id, quiche::Shutdown::Write, 268),
                Ok(())
            );
            helper.advance_and_run_loop().unwrap();

            // Server sees reset - drain any BodyBytesReceived events first
            loop {
                match helper.driver_recv_core_event() {
                    Ok(H3Event::ResetStream { stream_id: sid }) if sid == stream_id => break,
                    Ok(H3Event::BodyBytesReceived { .. }) => continue, // Drain body events
                    Ok(H3Event::StreamClosed { .. }) => continue, // Drain closure events
                    Ok(e) => panic!("Unexpected event: {:?}", e),
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            }
        }

        // Now create a new stream that should work without delays
        let final_stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), true)
            .unwrap();
        assert_eq!(final_stream_id, 12); // Next stream after 0, 4, 8

        helper.advance_and_run_loop().unwrap();

        // Server receives the new request
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req.stream_id, 12);

        // Server sends response with body
        let to_client = req.send.get_ref().unwrap().clone();
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        to_client
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(b"final response"),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client should receive headers and body without delay
        // Drain any pending events from cancelled streams first
        loop {
            match helper.peer_client_poll() {
                Ok((12, h3::Event::Headers { .. })) => break,
                Ok(_) => continue, // Drain events for other streams
                Err(h3::Error::Done) => panic!("Expected headers for stream 12"),
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
        assert_eq!(helper.peer_client_poll(), Ok((12, h3::Event::Data)));
        let body = helper.peer_client_recv_body_vec(12, 1024).unwrap();
        assert_eq!(body, b"final response");
        assert_eq!(helper.peer_client_poll(), Ok((12, h3::Event::Finished)));

        // Verify no stale streams in stream_map
        assert!(!helper.driver.stream_map.contains_key(&0));
        assert!(!helper.driver.stream_map.contains_key(&4));
        assert!(!helper.driver.stream_map.contains_key(&8));
    }

    /// Test that when a stream is reset while we're waiting for channel capacity,
    /// the waiting future is properly cleaned up and doesn't block new streams.
    #[test]
    fn reset_while_waiting_for_channel_capacity() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client sends request on stream 0
        let stream_id = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();
        assert_eq!(stream_id, 0);

        helper.advance_and_run_loop().unwrap();

        // Server receives request
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        let to_client = req.send.get_ref().unwrap().clone();
        let from_client = req.recv;

        // Server sends response headers
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives headers
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );

        // Client sends body data - this will fill the channel since capacity is 1 in tests
        helper.peer_client_send_body(0, &[1; 100], false).unwrap();
        helper.advance_and_run_loop().unwrap();

        // The driver should have data in waiting_streams now (blocked on channel capacity)
        // Don't consume from from_client yet to keep it blocked

        // Send more data to ensure we're blocked
        helper.peer_client_send_body(0, &[2; 100], false).unwrap();
        helper.pipe.advance().unwrap();
        helper.work_loop_iter().unwrap();

        // Now client sends RST_STREAM while we're blocked
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 268),
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // Server should receive reset event - drain any BodyBytesReceived events first
        loop {
            match helper.driver_recv_core_event() {
                Ok(H3Event::ResetStream { stream_id: 0 }) => break,
                Ok(H3Event::BodyBytesReceived { .. }) => continue, // Drain body events
                Ok(H3Event::StreamClosed { .. }) => continue, // Drain closure events
                Ok(e) => panic!("Unexpected event: {:?}", e),
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        // The from_client channel should be closed now
        assert!(from_client.is_closed());

        // Create a new stream - it should work without being blocked
        let stream_id_4 = helper
            .peer_client_send_request(make_request_headers("GET"), true)
            .unwrap();
        assert_eq!(stream_id_4, 4);

        helper.advance_and_run_loop().unwrap();

        // Server receives new request
        let req4 = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req4.stream_id, 4);

        // Server sends response
        let to_client_4 = req4.send.get_ref().unwrap().clone();
        to_client_4
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        to_client_4
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(b"response 4"),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives response on stream 4
        assert_matches!(
            helper.peer_client_poll(),
            Ok((4, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Data)));
        let body = helper.peer_client_recv_body_vec(4, 1024).unwrap();
        assert_eq!(body, b"response 4");
    }

    /// Test the exact scenario from the bug report: client starts reading from
    /// beginning of file, cancels mid-stream, then requests a range from the
    /// end of the file. The new range request should complete without delay.
    #[test]
    fn video_seek_scenario_cancel_and_range_request() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // === Initial request for full file (stream 0) ===
        let stream_0 = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();
        assert_eq!(stream_0, 0);

        helper.advance_and_run_loop().unwrap();

        let req0 = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        let to_client_0 = req0.send.get_ref().unwrap().clone();
        let audit_stats_0 = req0.h3_audit_stats.clone();

        // Server sends 200 OK headers
        to_client_0
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives headers
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );

        // Server starts streaming body (simulating video data)
        to_client_0
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&[0xAB; 82]),  // ~82KB like in bug report
                false,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives partial data
        assert_eq!(helper.peer_client_poll(), Ok((0, h3::Event::Data)));
        let partial_body = helper.peer_client_recv_body_vec(0, 1024).unwrap();
        assert!(!partial_body.is_empty());

        // === User seeks to end of video - client cancels stream 0 ===
        // This mirrors: RST_STREAM with ietf_error_code = 268 (H3_REQUEST_CANCELLED)
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 268),
            Ok(())
        );
        // Also send STOP_SENDING to indicate we don't want more data
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Read, 268),
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // Server receives reset - drain any BodyBytesReceived events first
        loop {
            match helper.driver_recv_core_event() {
                Ok(H3Event::ResetStream { stream_id: 0 }) => break,
                Ok(H3Event::BodyBytesReceived { .. }) => continue,
                Ok(H3Event::StreamClosed { .. }) => continue,
                Ok(e) => panic!("Unexpected event: {:?}", e),
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        // Verify audit stats recorded the reset
        assert_eq!(audit_stats_0.recvd_reset_stream_error_code(), 268);

        // === New range request for end of file (stream 4) ===
        // Request bytes 9338880-9362034 (end of file)
        let range_headers = vec![
            h3::Header::new(b":method", b"GET"),
            h3::Header::new(b":scheme", b"https"),
            h3::Header::new(b":authority", b"quic.tech"),
            h3::Header::new(b":path", b"/test"),
            h3::Header::new(b"range", b"bytes=9338880-9362034"),
        ];
        let stream_4 = helper
            .peer
            .send_request(&mut helper.pipe.client, &range_headers, true)
            .unwrap();
        assert_eq!(stream_4, 4);

        helper.advance_and_run_loop().unwrap();

        // Server receives range request
        let req4 = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req4.stream_id, 4);
        let to_client_4 = req4.send.get_ref().unwrap().clone();

        // Server sends 206 Partial Content headers (like CF would)
        // Note: Using smaller body size due to test flow control limits
        let response_206_headers = vec![
            h3::Header::new(b":status", b"206"),
            h3::Header::new(b"content-type", b"video/quicktime"),
            h3::Header::new(b"content-length", b"50"),
            h3::Header::new(b"content-range", b"bytes 9338880-9338929/9362035"),
        ];
        to_client_4
            .try_send(OutboundFrame::Headers(response_206_headers, None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives 206 headers - THIS HAPPENS IMMEDIATELY IN THE BUG
        assert_matches!(
            helper.peer_client_poll(),
            Ok((4, h3::Event::Headers { .. }))
        );

        // Server sends body - THIS SHOULD NOT BE DELAYED
        // In the bug, this was delayed 60 seconds
        // Using smaller body due to test flow control limits
        let range_body = vec![0xCD; 50];
        to_client_4
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(&range_body),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client should receive body data IMMEDIATELY (not after 60 seconds)
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Data)));
        let received_body = helper.peer_client_recv_body_vec(4, 1024).unwrap();
        assert_eq!(received_body.len(), 50);
        assert_eq!(received_body, range_body);

        // Stream 4 should complete
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Finished)));

        // Verify stream 0 received the reset (the read direction is closed).
        // Note: The stream may still be in the map if the server hasn't explicitly
        // closed its write direction, but it has received the client's reset.
        if let Some(ctx) = helper.driver.stream_map.get(&0) {
            assert!(ctx.fin_or_reset_recv, "Stream 0 should have received reset");
        }
        // If not in the map, it was fully cleaned up which is also fine
    }

    /// Test that close_waiting_stream_channels properly closes channels when
    /// a stream is reset, preventing waiting futures from blocking forever.
    #[test]
    fn close_waiting_stream_channels_on_reset() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client sends request with body
        let stream_id = helper
            .peer_client_send_request(make_request_headers("POST"), false)
            .unwrap();
        assert_eq!(stream_id, 0);

        helper.advance_and_run_loop().unwrap();

        // Server receives request
        let req = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        let to_client = req.send.get_ref().unwrap().clone();

        // Server sends response headers
        to_client
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives headers
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );

        // Client sends body to fill the channel
        helper.peer_client_send_body(0, &[1; 50], false).unwrap();
        helper.pipe.advance().unwrap();
        helper.work_loop_iter().unwrap();

        // At this point, the driver should have a waiting future for this stream
        // if the channel is at capacity

        // Client sends RST_STREAM
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 268),
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // The reset should have been processed - drain any BodyBytesReceived events first
        loop {
            match helper.driver_recv_core_event() {
                Ok(H3Event::ResetStream { stream_id: 0 }) => break,
                Ok(H3Event::BodyBytesReceived { .. }) => continue, // Drain body events
                Ok(H3Event::StreamClosed { .. }) => continue, // Drain closure events
                Ok(e) => panic!("Unexpected event: {:?}", e),
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        // After reset, any waiting futures for stream 0 should be cleaned up
        // Create another stream to verify the system isn't blocked
        let stream_4 = helper
            .peer_client_send_request(make_request_headers("GET"), true)
            .unwrap();
        assert_eq!(stream_4, 4);

        helper.advance_and_run_loop().unwrap();

        let req4 = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req4.stream_id, 4);

        // Verify stream 4 can complete normally
        let to_client_4 = req4.send.get_ref().unwrap().clone();
        to_client_4
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();
        to_client_4
            .try_send(OutboundFrame::Body(BufFactory::get_empty_buf(), true))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        assert_matches!(
            helper.peer_client_poll(),
            Ok((4, h3::Event::Headers { .. }))
        );
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Data)));
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Finished)));
    }

    /// Test concurrent streams where one is cancelled - ensures the other
    /// stream continues to receive data without interference.
    #[test]
    fn concurrent_streams_one_cancelled() {
        let mut helper = DriverTestHelper::<ServerHooks>::new().unwrap();
        helper.complete_handshake().unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client sends two concurrent requests
        let stream_0 = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();
        let stream_4 = helper
            .peer_client_send_request(make_request_headers("GET"), false)
            .unwrap();
        assert_eq!(stream_0, 0);
        assert_eq!(stream_4, 4);

        helper.advance_and_run_loop().unwrap();

        // Server receives both requests
        let req0 = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req0.stream_id, 0);
        let to_client_0 = req0.send.get_ref().unwrap().clone();

        let req4 = assert_matches!(
            helper.driver_recv_server_event().unwrap(),
            ServerH3Event::Headers{incoming_headers, ..} => { incoming_headers }
        );
        assert_eq!(req4.stream_id, 4);
        let to_client_4 = req4.send.get_ref().unwrap().clone();

        // Server sends response headers on both streams
        to_client_0
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        to_client_4
            .try_send(OutboundFrame::Headers(make_response_headers(), None))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives headers on both
        assert_matches!(
            helper.peer_client_poll(),
            Ok((0, h3::Event::Headers { .. }))
        );
        assert_matches!(
            helper.peer_client_poll(),
            Ok((4, h3::Event::Headers { .. }))
        );

        // Client cancels stream 0
        assert_eq!(
            helper
                .pipe
                .client
                .stream_shutdown(0, quiche::Shutdown::Write, 268),
            Ok(())
        );
        helper.advance_and_run_loop().unwrap();

        // Server sees reset on stream 0
        assert_matches!(
            helper.driver_recv_core_event(),
            Ok(H3Event::ResetStream { stream_id: 0 })
        );

        // Stream 4 should continue to work normally
        to_client_4
            .try_send(OutboundFrame::Body(
                BufFactory::buf_from_slice(b"stream 4 body"),
                true,
            ))
            .unwrap();
        helper.advance_and_run_loop().unwrap();

        // Client receives body on stream 4 without delay
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Data)));
        let body = helper.peer_client_recv_body_vec(4, 1024).unwrap();
        assert_eq!(body, b"stream 4 body");
        assert_eq!(helper.peer_client_poll(), Ok((4, h3::Event::Finished)));

        // Stream 0 should be cleaned up
        helper.advance_and_run_loop().unwrap();
        assert!(!helper.driver.stream_map.contains_key(&0) ||
                helper.driver.stream_map.get(&0).map(|s| s.fin_or_reset_recv).unwrap_or(true));
    }
}
