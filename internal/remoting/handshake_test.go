package remoting

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
	"google.golang.org/protobuf/proto"
)

func handshakeTCPPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return client, server
}

func readHandshakeFrame(conn net.Conn) error {
	var size uint32
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		return err
	}
	_, err := io.CopyN(io.Discard, conn, int64(size))
	return err
}

func handshakeResult(t *testing.T, ch <-chan error, limit time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(limit):
		t.Fatal("handshake did not stop while the peer remained silent")
		return nil
	}
}

func TestHandshakeContextDeadlineInterruptsSilentPeer(t *testing.T) {
	client, server := handshakeTCPPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- Handshake(ctx, client) }()
	if err := readHandshakeFrame(server); err != nil {
		t.Fatal(err)
	}
	err := handshakeResult(t, result, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context deadline", err)
	}
}

func TestHandshakeDefaultDeadlineInterruptsSilentPeer(t *testing.T) {
	client, server := handshakeTCPPair(t)
	result := make(chan error, 1)
	go func() { result <- Handshake(context.Background(), client) }()
	if err := readHandshakeFrame(server); err != nil {
		t.Fatal(err)
	}
	err := handshakeResult(t, result, 6*time.Second)
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("got %v, want timeout", err)
	}
}

func TestHandshakeCancellationInterruptsRead(t *testing.T) {
	client, server := handshakeTCPPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- Handshake(ctx, client) }()
	if err := readHandshakeFrame(server); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := handshakeResult(t, result, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
}

type handshakePipeConn struct {
	net.Conn
	writing chan struct{}
}

func (c handshakePipeConn) Write(data []byte) (int, error) {
	close(c.writing)
	return c.Conn.Write(data)
}

func (c handshakePipeConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}

func TestHandshakeCancellationInterruptsWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	writing := make(chan struct{})
	go func() { result <- Handshake(ctx, handshakePipeConn{client, writing}) }()
	// Wait until the real net.Pipe write is admitted; the peer never reads it.
	select {
	case <-writing:
	case <-time.After(time.Second):
		t.Fatal("handshake never reached socket write")
	}
	cancel()
	if err := handshakeResult(t, result, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
}

func TestHandshakeContextDeadlineInterruptsWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- Handshake(ctx, handshakePipeConn{client, make(chan struct{})}) }()
	if err := handshakeResult(t, result, time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context deadline during write", err)
	}
}

func TestHandshakeRejectsTruncatedReplyPromptly(t *testing.T) {
	for name, reply := range map[string][]byte{"header": {0, 0}, "payload": {0, 0, 0, 4, 1}} {
		t.Run(name, func(t *testing.T) {
			client, server := handshakeTCPPair(t)
			result := make(chan error, 1)
			go func() { result <- Handshake(context.Background(), client) }()
			if err := readHandshakeFrame(server); err != nil {
				t.Fatal(err)
			}
			if _, err := server.Write(reply); err != nil {
				t.Fatal(err)
			}
			if err := server.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if err := handshakeResult(t, result, time.Second); err == nil {
				t.Fatal("accepted truncated handshake")
			}
		})
	}
}

func TestHandshakeSuccessLeavesConnectionUsableAfterCancellation(t *testing.T) {
	client, server := handshakeTCPPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- Handshake(ctx, client) }()
	if err := readHandshakeFrame(server); err != nil {
		t.Fatal(err)
	}
	// Preserve waiting through other control frames before the association ACK.
	for _, command := range []akka.CommandType{akka.CommandType_HEARTBEAT, akka.CommandType_ASSOCIATE} {
		data, err := proto.Marshal(&akka.AkkaProtocolMessage{Instruction: &akka.AkkaControlMessage{CommandType: command.Enum()}})
		if err != nil {
			t.Fatal(err)
		}
		if err := binary.Write(server, binary.BigEndian, uint32(len(data))); err != nil {
			t.Fatal(err)
		}
		if _, err := server.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := handshakeResult(t, result, time.Second); err != nil {
		t.Fatal(err)
	}
	cancel()
	// Also pass the original context deadline: a successful exchange must clear it.
	time.Sleep(120 * time.Millisecond)
	if _, err := server.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	var buf [2]byte
	if _, err := io.ReadFull(client, buf[:]); err != nil {
		t.Fatal(err)
	}
	if string(buf[:]) != "ok" {
		t.Fatalf("unexpected application bytes %q", buf)
	}
}
