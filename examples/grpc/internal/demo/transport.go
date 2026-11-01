package demo

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"

	"google.golang.org/grpc/credentials"
)

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func roots(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("could not read CA bundle")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("CA bundle has no valid certificates")
	}
	return pool, nil
}

func transport(o CommonOptions, address string, server bool) (credentials.TransportCredentials, error) {
	if (o.Cert == "") != (o.Key == "") {
		return nil, errors.New("-cert and -key must be provided together")
	}
	if o.Insecure {
		if !loopbackAddress(address) {
			return nil, errors.New("-insecure requires a loopback host:port")
		}
		if o.Cert != "" || o.CA != "" || o.ServerName != "" {
			return nil, errors.New("TLS flags cannot be combined with -insecure")
		}
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: o.ServerName}
	if o.Cert != "" {
		cert, err := tls.LoadX509KeyPair(o.Cert, o.Key)
		if err != nil {
			return nil, errors.New("could not load TLS certificate/key")
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if server {
		if o.Cert == "" {
			return nil, errors.New("server TLS requires -cert and -key; use -insecure only for loopback development")
		}
		if o.ServerName != "" {
			return nil, errors.New("-server-name is client-only")
		}
		if o.CA != "" {
			pool, err := roots(o.CA)
			if err != nil {
				return nil, err
			}
			cfg.ClientCAs = pool
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
	} else if o.CA != "" {
		pool, err := roots(o.CA)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	return credentials.NewTLS(cfg), nil
}
