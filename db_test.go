package rendezvous

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestPackAddrs(t *testing.T) {
	addrs := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		addrs[i] = make([]byte, rand.Intn(256))
	}

	packed := packAddrs(addrs)
	unpacked, err := unpackAddrs(packed)
	if err != nil {
		t.Fatal(err)
	}

	if len(addrs) != len(unpacked) {
		t.Fatal("unpacked address length mismatch")
	}

	for i, addr := range addrs {
		if !bytes.Equal(addr, unpacked[i]) {
			t.Fatal("unpacked addr not equal to original")
		}
	}
}

func TestPackCookie(t *testing.T) {
	nonce := make([]byte, 16)
	_, err := rand.Read(nonce)
	if err != nil {
		t.Fatal(err)
	}

	counter := rand.Int63()
	ns := "blah"

	cookie := packCookie(counter, ns, nonce)

	if !validCookie(cookie, ns, nonce) {
		t.Fatal("packed an invalid cookie")
	}

	xcounter, err := unpackCookie(cookie)
	if err != nil {
		t.Fatal(err)
	}

	if counter != xcounter {
		t.Fatal("unpacked cookie counter not equal to original")
	}
}
