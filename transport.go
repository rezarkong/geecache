package geecache

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

const mutationCodecName = "json"

// TLSOptions controls both the gRPC server credentials and peer-to-peer client dialing.
type TLSOptions struct {
	CertFile           string
	KeyFile            string
	CAFile             string
	ServerName         string
	InsecureSkipVerify bool
}

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (jsonCodec) Name() string {
	return mutationCodecName
}

func buildPeerDialOptions(tlsOpts *TLSOptions, extras []grpc.DialOption) ([]grpc.DialOption, error) {
	opts := append([]grpc.DialOption(nil), extras...)
	if len(opts) > 0 {
		return opts, nil
	}

	creds, err := loadClientTransportCredentials(tlsOpts)
	if err != nil {
		return nil, err
	}
	return append(opts, grpc.WithTransportCredentials(creds)), nil
}

func loadServerTransportCredentials(tlsOpts *TLSOptions) (credentials.TransportCredentials, error) {
	if tlsOpts == nil {
		return nil, nil
	}
	if tlsOpts.CertFile == "" || tlsOpts.KeyFile == "" {
		return nil, fmt.Errorf("tls cert and key are required for server credentials")
	}

	certificate, err := tls.LoadX509KeyPair(tlsOpts.CertFile, tlsOpts.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server tls key pair: %w", err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}
	if tlsOpts.CAFile != "" {
		caPEM, err := os.ReadFile(tlsOpts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read server tls ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("append server tls ca file")
		}
		config.ClientCAs = pool
		config.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return credentials.NewTLS(config), nil
}

func loadClientTransportCredentials(tlsOpts *TLSOptions) (credentials.TransportCredentials, error) {
	if tlsOpts == nil {
		return insecure.NewCredentials(), nil
	}

	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         tlsOpts.ServerName,
		InsecureSkipVerify: tlsOpts.InsecureSkipVerify,
	}

	if tlsOpts.CAFile != "" {
		caPEM, err := os.ReadFile(tlsOpts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read client tls ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("append client tls ca file")
		}
		config.RootCAs = pool
	}

	if tlsOpts.CertFile != "" || tlsOpts.KeyFile != "" {
		if tlsOpts.CertFile == "" || tlsOpts.KeyFile == "" {
			return nil, fmt.Errorf("both client cert and key are required")
		}
		certificate, err := tls.LoadX509KeyPair(tlsOpts.CertFile, tlsOpts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client tls key pair: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}

	return credentials.NewTLS(config), nil
}
