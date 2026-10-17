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

package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.ate.dev/substrate/internal/grpcecho"
)

// grpcTLSCredentials loads the origin certificate and key for the gRPC server,
// offering only h2 over TLS. Both are empty for a cleartext-only server, which
// returns nil credentials. Invalid credentials fail setup before any listener
// starts.
func grpcTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("--tls-cert and --tls-key must be provided together")
	}

	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading TLS certificate and key: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	}), nil
}

// newHealthHandler answers the HTTP readiness probe. Deliberately trivial: it
// reports that the process is up, and the gRPC listener is opened before this
// one, so a 200 here means the RPC port is already accepting.
func newHealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	return mux
}

// newGRPCCmd is the gRPC server both networking e2e suites talk to. One
// subcommand serves both directions so the two legs cannot drift apart in what
// they consider a working RPC:
//
//   - egress: deployed as a plain pod, it is the origin an Actor dials through
//     the egress gateway.
//   - ingress: deployed as an Actor, it is the origin a client reaches through
//     atenet-router.
//
// It serves cleartext HTTP/2 by default, and given --tls-cert and --tls-key it
// also serves TLS (ALPN h2) on :8443 beside --listen. The egress pod runs
// without a certificate because the leg under test is the tunnel, not the
// origin's identity, and because the egress gateway relays a terminated CONNECT
// as opaque TCP: whatever the actor speaks is what arrives here.
//
// Echo, EchoStream and EchoBidi are implemented in internal/grpcecho, which
// also reads the per-stream controls EchoBidi takes as request metadata.
//
// It answers the grpc health service as well as Echo, which is what the egress
// pod's readinessProbe checks. Multiplexing readiness onto that listener as an
// h2c handler would defeat the point of the egress fixture, where nothing
// between the actor and here parses HTTP -- so --health-listen puts it on a
// second port instead, off unless asked for. The ingress Actor needs it because
// an ActorTemplate's wakeupProbe is an HTTP GET and nothing else: a gRPC server
// answers one with a protocol error, so without it the Actor never boots.
func newGRPCCmd() *cobra.Command {
	var listenAddress string
	var healthAddress string
	var certFile, keyFile string
	cmd := &cobra.Command{
		Use:   "grpc",
		Short: "Serve a cleartext HTTP/2 gRPC echo origin, plus TLS given a certificate.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			creds, err := grpcTLSCredentials(certFile, keyFile)
			if err != nil {
				return err
			}

			cleartext, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return fmt.Errorf("listening on %s: %w", listenAddress, err)
			}
			log.Printf("testserver grpc: serving on %s", cleartext.Addr())

			// Every listener is bound before any Serve starts, so a readiness
			// 200 can never precede the gRPC ports being open.
			var encrypted net.Listener
			if creds != nil {
				if encrypted, err = net.Listen("tcp", tlsListenAddress); err != nil {
					_ = cleartext.Close()
					return fmt.Errorf("listening for TLS on %s: %w", tlsListenAddress, err)
				}
				log.Printf("testserver grpc: serving gRPC over TLS on %s", tlsListenAddress)
			}

			if healthAddress != "" {
				healthListener, err := net.Listen("tcp", healthAddress)
				if err != nil {
					_ = cleartext.Close()
					if encrypted != nil {
						_ = encrypted.Close()
					}
					return fmt.Errorf("listening for health on %s: %w", healthAddress, err)
				}
				log.Printf("testserver grpc: serving /readyz on %s", healthListener.Addr())
				go func() {
					log.Fatal(http.Serve(healthListener, newHealthHandler()))
				}()
			}

			errs := make(chan error, 2)
			go func() { errs <- grpcecho.NewServer().Serve(cleartext) }()
			if encrypted != nil {
				go func() { errs <- grpcecho.NewServer(grpc.Creds(creds)).Serve(encrypted) }()
			}
			return <-errs
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":50051", "Address the gRPC server listens on, cleartext HTTP/2.")
	cmd.Flags().StringVar(&healthAddress, "health-listen", "", "Address for an HTTP/1.1 /readyz listener. Empty serves no HTTP at all, which is what the egress fixture wants; the ingress Actor sets it because an ActorTemplate wakeupProbe is an HTTP GET.")
	cmd.Flags().StringVar(&certFile, "tls-cert", "", "PEM certificate file; with --tls-key, also serves gRPC over TLS on :8443.")
	cmd.Flags().StringVar(&keyFile, "tls-key", "", "PEM private key file; with --tls-cert, also serves gRPC over TLS on :8443.")
	return cmd
}
