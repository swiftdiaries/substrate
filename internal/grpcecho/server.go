// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package grpcecho is the Echo service behind the e2e gRPC origins, with
// per-stream controls passed as request metadata.
package grpcecho

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/proto/grpcechopb"
)

// Metadata keys that control an EchoBidi stream. Each is an integer unless
// noted, and the key pairs must be given together.
const (
	FailAfterKey       = "echo-fail-after"        // int >= 0: after this many responses, end the stream with FailCodeKey
	FailCodeKey        = "echo-fail-code"         // int 1..16 (codes.Code); required with FailAfterKey
	IdleAfterKey       = "echo-idle-after"        // int >= 1: after this many responses, pause IdleMsKey once
	IdleMsKey          = "echo-idle-ms"           // int 1..600000; required with IdleAfterKey
	ReplyBytesKey      = "echo-reply-bytes"       // int 1..MaxPayloadBytes: pad each response message to this size
	AfterCloseCountKey = "echo-after-close-count" // int 0..MaxStreamCount: responses sent after the client half-closes
	StreamIDKey        = "echo-stream-id"         // records how this EchoBidi ended
	OutcomeOfKey       = "echo-outcome-of"        // on unary Echo: reply with the recorded outcome of that stream ID
)

// Outcomes recorded for an EchoBidi stream carrying StreamIDKey.
const (
	OutcomeEOF      = "eof"
	OutcomeCanceled = "canceled"
	OutcomeError    = "error"
)

const (
	// MaxStreamCount bounds EchoStream and the after-close responses. A test
	// asks for a handful of messages; a request for millions is a bug in the
	// caller, and answering it would look like a hung tunnel rather than the
	// mistake it is.
	MaxStreamCount = 1000

	// MaxPayloadBytes bounds the size a response message is padded to.
	MaxPayloadBytes = 2 << 20

	// PadByte fills the padding added by Pad.
	PadByte = '.'
)

// maxOutcomes bounds the outcome table. When it is full it is cleared, so an
// outcome can be reported as unknown after enough newer streams have ended.
const maxOutcomes = 1024

// Pad returns message followed by PadByte until it is size bytes long. A
// message already at least size bytes long is returned unchanged.
func Pad(message string, size int) string {
	if len(message) >= size {
		return message
	}
	return message + strings.Repeat(string(PadByte), size-len(message))
}

// Server implements the Echo service.
type Server struct {
	grpcechopb.UnimplementedEchoServer

	mu       sync.Mutex
	outcomes map[string]string
}

// NewServer returns a gRPC server with the Echo and health services registered,
// so the unit tests exercise the same registrations the pod serves.
func NewServer(opts ...grpc.ServerOption) *grpc.Server {
	server := grpc.NewServer(opts...)
	grpcechopb.RegisterEchoServer(server, &Server{})
	healthpb.RegisterHealthServer(server, health.NewServer())
	return server
}

// Echo returns the message it was sent. With OutcomeOfKey in its metadata it
// returns the recorded outcome of that stream instead, which is empty when the
// stream is unknown or still running.
func (s *Server) Echo(ctx context.Context, req *grpcechopb.EchoRequest) (*grpcechopb.EchoResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if ids := md.Get(OutcomeOfKey); len(ids) > 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
		return &grpcechopb.EchoResponse{Message: s.outcomes[ids[0]]}, nil
	}
	return &grpcechopb.EchoResponse{Message: req.GetMessage()}, nil
}

// EchoStream returns the message it was sent count times, numbered by position.
func (s *Server) EchoStream(req *grpcechopb.EchoStreamRequest, stream grpc.ServerStreamingServer[grpcechopb.EchoResponse]) error {
	count := req.GetCount()
	if count <= 0 {
		return status.Errorf(codes.InvalidArgument, "count must be positive, got %d", count)
	}
	if count > MaxStreamCount {
		return status.Errorf(codes.InvalidArgument, "count must be at most %d, got %d", MaxStreamCount, count)
	}
	for i := range count {
		if err := stream.Send(&grpcechopb.EchoResponse{Message: req.GetMessage(), Index: i}); err != nil {
			return err
		}
	}
	return nil
}

// EchoBidi answers each request as it arrives rather than draining the request
// direction first. Batching the responses would deadlock a caller that waits
// for each one before sending the next, and would also stop this from testing
// anything a server-stream does not already cover: the point is frames moving
// both ways at once.
//
// Its behavior is shaped by the metadata keys above. Without them it echoes
// every request and ends OK when the client half-closes.
func (s *Server) EchoBidi(stream grpc.BidiStreamingServer[grpcechopb.EchoRequest, grpcechopb.EchoResponse]) (err error) {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if ids := md.Get(StreamIDKey); len(ids) > 0 {
		defer func() {
			switch {
			case err == nil:
				s.record(ids[0], OutcomeEOF)
			case stream.Context().Err() != nil:
				s.record(ids[0], OutcomeCanceled)
			default:
				s.record(ids[0], OutcomeError)
			}
		}()
	}

	c, err := parseControls(md)
	if err != nil {
		return err
	}

	for index := int32(0); ; index++ {
		if int(index) == c.failAfter {
			return status.Errorf(c.failCode, "failing after %d responses as requested", index)
		}

		req, recvErr := stream.Recv()
		// io.EOF is the client's half-close: the request direction is done,
		// this direction still has to end cleanly with an OK status.
		if errors.Is(recvErr, io.EOF) {
			for j := range c.afterClose {
				message := Pad(fmt.Sprintf("after-close-%d", j), c.replyBytes)
				if err := stream.Send(&grpcechopb.EchoResponse{Message: message, Index: index + int32(j)}); err != nil {
					return err
				}
			}
			return nil
		}
		if recvErr != nil {
			return recvErr
		}

		message := Pad(req.GetMessage(), c.replyBytes)
		if err := stream.Send(&grpcechopb.EchoResponse{Message: message, Index: index}); err != nil {
			return err
		}

		if int(index)+1 == c.idleAfter {
			select {
			case <-time.After(c.idle):
			case <-stream.Context().Done():
				return status.FromContextError(stream.Context().Err()).Err()
			}
		}
	}
}

// record stores how the stream with the given ID ended.
func (s *Server) record(id, outcome string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.outcomes == nil {
		s.outcomes = make(map[string]string)
	}
	if len(s.outcomes) >= maxOutcomes {
		clear(s.outcomes)
	}
	s.outcomes[id] = outcome
}

// controls is the per-stream behavior an EchoBidi call asks for. Unset fields
// hold the zero value, except failAfter and idleAfter which are -1.
type controls struct {
	failAfter  int
	failCode   codes.Code
	idleAfter  int
	idle       time.Duration
	replyBytes int
	afterClose int
}

// parseControls reads the control keys from md. A malformed or out-of-range
// value, or one key of a pair given without the other, is an InvalidArgument
// error rather than a silent default, so a caller with a typo sees the typo.
func parseControls(md metadata.MD) (controls, error) {
	c := controls{failAfter: -1, idleAfter: -1}

	failAfter, hasFailAfter, err := intControl(md, FailAfterKey, 0, math.MaxInt32)
	if err != nil {
		return controls{}, err
	}
	failCode, hasFailCode, err := intControl(md, FailCodeKey, 1, 16)
	if err != nil {
		return controls{}, err
	}
	if err := requireTogether(FailAfterKey, hasFailAfter, FailCodeKey, hasFailCode); err != nil {
		return controls{}, err
	}
	if hasFailAfter {
		c.failAfter = failAfter
		c.failCode = codes.Code(failCode)
	}

	idleAfter, hasIdleAfter, err := intControl(md, IdleAfterKey, 1, math.MaxInt32)
	if err != nil {
		return controls{}, err
	}
	idleMs, hasIdleMs, err := intControl(md, IdleMsKey, 1, 600000)
	if err != nil {
		return controls{}, err
	}
	if err := requireTogether(IdleAfterKey, hasIdleAfter, IdleMsKey, hasIdleMs); err != nil {
		return controls{}, err
	}
	if hasIdleAfter {
		c.idleAfter = idleAfter
		c.idle = time.Duration(idleMs) * time.Millisecond
	}

	if c.replyBytes, _, err = intControl(md, ReplyBytesKey, 1, MaxPayloadBytes); err != nil {
		return controls{}, err
	}
	if c.afterClose, _, err = intControl(md, AfterCloseCountKey, 0, MaxStreamCount); err != nil {
		return controls{}, err
	}
	return c, nil
}

// intControl reads key from md as an integer in [lo, hi]. The bool reports
// whether the key was given at all; an absent key is not an error.
func intControl(md metadata.MD, key string, lo, hi int) (int, bool, error) {
	values := md.Get(key)
	switch {
	case len(values) == 0:
		return 0, false, nil
	case len(values) > 1:
		return 0, true, status.Errorf(codes.InvalidArgument, "%s given %d times, want once", key, len(values))
	}
	n, err := strconv.Atoi(values[0])
	if err != nil || n < lo || n > hi {
		return 0, true, status.Errorf(codes.InvalidArgument, "%s must be an integer in [%d, %d], got %q", key, lo, hi, values[0])
	}
	return n, true, nil
}

// requireTogether rejects a control given without its partner.
func requireTogether(keyA string, hasA bool, keyB string, hasB bool) error {
	if hasA != hasB {
		return status.Errorf(codes.InvalidArgument, "%s and %s must be given together", keyA, keyB)
	}
	return nil
}
