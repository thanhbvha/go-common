package crypt

import (
	"testing"
)

func TestHashes(t *testing.T) {
	data := []byte("hello world")

	md5Hash := HashMD5(data)
	if md5Hash != "5eb63bbbe01eeed093cb22bb8f5acdc3" {
		t.Errorf("unexpected md5: %s", md5Hash)
	}

	sha1Hash := HashSHA1(data)
	if sha1Hash != "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed" {
		t.Errorf("unexpected sha1: %s", sha1Hash)
	}

	sha256Hash := HashSHA256(data)
	if sha256Hash != "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9" {
		t.Errorf("unexpected sha256: %s", sha256Hash)
	}

	sha512Hash := HashSHA512(data)
	if len(sha512Hash) != 128 { // 64 bytes in hex
		t.Errorf("unexpected sha512 length: %d", len(sha512Hash))
	}

	sha3Hash := HashSHA3_256(data)
	if sha3Hash != "644bcc7e564373040999aac89e7622f3ca71fba1d972fd94a31c3bfbf24e3938" {
		t.Errorf("unexpected sha3_256: %s", sha3Hash)
	}

	blake2bHash, err := HashBLAKE2b(data)
	if err != nil {
		t.Fatalf("unexpected blake2b error: %v", err)
	}
	if len(blake2bHash) != 64 { // 32 bytes in hex
		t.Errorf("unexpected blake2b length: %d", len(blake2bHash))
	}
	if blake2bHash != "256c83b297114d201b30179f3f0ef0cace9783622da5974326b436178aeef610" {
		t.Errorf("unexpected blake2b hash: %s", blake2bHash)
	}
}
