// Package devcert creates short-lived localhost certificates for this example.
// These certificates and their local CA must never be used for production.
package devcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Generate creates a NEW directory and refuses to overwrite existing files.
// Go's file mode bits do not set Windows ACLs; protect the directory yourself.
func Generate(directory string) error {
	if err := os.Mkdir(directory, 0700); err != nil {
		return errors.New("create a new certificate directory; parent must exist and destination must not exist")
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	ca, err := template("gRPC example development CA")
	if err != nil {
		return err
	}
	ca.IsCA = true
	ca.BasicConstraintsValid = true
	ca.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	if err = write(directory, "ca.pem", "CERTIFICATE", caDER); err != nil {
		return err
	}
	// The CA private key is kept in memory only, so this CA cannot issue more certs.
	for _, name := range []string{"server", "client"} {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		cert, err := template("gRPC example " + name)
		if err != nil {
			return err
		}
		cert.KeyUsage = x509.KeyUsageDigitalSignature
		if name == "server" {
			cert.DNSNames = []string{"localhost"}
			cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		} else {
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if err != nil {
			return err
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return err
		}
		if err = write(directory, name+".pem", "CERTIFICATE", der); err != nil {
			return err
		}
		if err = write(directory, name+"-key.pem", "PRIVATE KEY", keyDER); err != nil {
			return err
		}
	}
	return nil
}

func template(name string) (*x509.Certificate, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	return &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour)}, nil
}

func write(directory, name, kind string, der []byte) error {
	f, err := os.OpenFile(filepath.Join(directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	encodeErr := pem.Encode(f, &pem.Block{Type: kind, Bytes: der})
	return errors.Join(encodeErr, f.Close())
}
