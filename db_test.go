package rendezvous

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
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

	if !equalAddrs(addrs, unpacked) {
		t.Fatal("unpacked addr not equal to original")
	}
}

func equalAddrs(addrs1, addrs2 [][]byte) bool {
	if len(addrs1) != len(addrs2) {
		return false
	}

	for i, addr1 := range addrs1 {
		addr2 := addrs2[i]
		if !bytes.Equal(addr1, addr2) {
			return false
		}
	}

	return true
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

func TestOpenCloseMemDB(t *testing.T) {
	db, err := OpenDB(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// let the flush goroutine run its cleanup act
	time.Sleep(1 * time.Second)

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenCloseFSDB(t *testing.T) {
	db, err := OpenDB(context.Background(), "/tmp/rendezvous-test.db")
	if err != nil {
		t.Fatal(err)
	}

	nonce1 := db.nonce

	// let the flush goroutine run its cleanup act
	time.Sleep(1 * time.Second)

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	db, err = OpenDB(context.Background(), "/tmp/rendezvous-test.db")
	if err != nil {
		t.Fatal(err)
	}

	nonce2 := db.nonce

	// let the flush goroutine run its cleanup act
	time.Sleep(1 * time.Second)

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(nonce1, nonce2) {
		t.Fatal("persistent db nonces are not equal")
	}
}

func TestDBRegistrationAndDiscovery(t *testing.T) {
	db, err := OpenDB(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	p1, err := peer.IDB58Decode("QmVr26fY1tKyspEJBniVhqxQeEjhF78XerGiqWAwraVLQH")
	if err != nil {
		t.Fatal(err)
	}

	p2, err := peer.IDB58Decode("QmUkUQgxXeggyaD5Ckv8ZqfW8wHBX6cYyeiyqvVZYzq5Bi")
	if err != nil {
		t.Fatal(err)
	}

	addr1, err := ma.NewMultiaddr("/ip4/1.1.1.1/tcp/9999")
	if err != nil {
		t.Fatal(err)
	}
	addrs1 := [][]byte{addr1.Bytes()}

	addr2, err := ma.NewMultiaddr("/ip4/2.2.2.2/tcp/9999")
	if err != nil {
		t.Fatal(err)
	}
	addrs2 := [][]byte{addr2.Bytes()}

	// register p1 and do discovery
	err = db.Register(p1, "foo1", addrs1, 60)
	if err != nil {
		t.Fatal(err)
	}

	count, err := db.CountRegistrations(p1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("registrations for p1 should be 1")
	}

	rrs, cookie, err := db.Discover("foo1", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 1 {
		t.Fatal("should have got 1 registration")
	}
	rr := rrs[0]
	if rr.Id != p1 {
		t.Fatal("expected p1 ID in registration")
	}
	if !equalAddrs(rr.Addrs, addrs1) {
		t.Fatal("expected p1's addrs in registration")
	}

	// register p2 and do progressive discovery
	err = db.Register(p2, "foo1", addrs2, 60)
	if err != nil {
		t.Fatal(err)
	}

	count, err = db.CountRegistrations(p2)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("registrations for p2 should be 1")
	}

	rrs, cookie, err = db.Discover("foo1", cookie, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 1 {
		t.Fatal("should have got 1 registration")
	}
	rr = rrs[0]
	if rr.Id != p2 {
		t.Fatal("expected p2 ID in registration")
	}
	if !equalAddrs(rr.Addrs, addrs2) {
		t.Fatal("expected p2's addrs in registration")
	}

	// reregister p1 and do progressive discovery
	err = db.Register(p1, "foo1", addrs1, 60)
	if err != nil {
		t.Fatal(err)
	}

	count, err = db.CountRegistrations(p1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("registrations for p1 should be 1")
	}

	rrs, cookie, err = db.Discover("foo1", cookie, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 1 {
		t.Fatal("should have got 1 registration")
	}
	rr = rrs[0]
	if rr.Id != p1 {
		t.Fatal("expected p1 ID in registration")
	}
	if !equalAddrs(rr.Addrs, addrs1) {
		t.Fatal("expected p1's addrs in registration")
	}

	// do a full discovery
	rrs, _, err = db.Discover("foo1", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 2 {
		t.Fatal("should have got 2 registration")
	}
	rr = rrs[0]
	if rr.Id != p2 {
		t.Fatal("expected p2 ID in registration")
	}
	if !equalAddrs(rr.Addrs, addrs2) {
		t.Fatal("expected p2's addrs in registration")
	}

	rr = rrs[1]
	if rr.Id != p1 {
		t.Fatal("expected p1 ID in registration")
	}
	if !equalAddrs(rr.Addrs, addrs1) {
		t.Fatal("expected p1's addrs in registration")
	}

	// unregister p2 and redo discovery
	err = db.Unregister(p2, "foo1")
	if err != nil {
		t.Fatal(err)
	}

	count, err = db.CountRegistrations(p2)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("registrations for p2 should be 0")
	}

	rrs, _, err = db.Discover("foo1", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 1 {
		t.Fatal("should have got 1 registration")
	}
	rr = rrs[0]
	if rr.Id != p1 {
		t.Fatal("expected p1 ID in registration")
	}
	if !equalAddrs(rr.Addrs, addrs1) {
		t.Fatal("expected p1's addrs in registration")
	}

	db.Close()
}

func TestDBRegistrationAndDiscoveryMultipleNS(t *testing.T) {
	db, err := OpenDB(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	p1, err := peer.IDB58Decode("QmVr26fY1tKyspEJBniVhqxQeEjhF78XerGiqWAwraVLQH")
	if err != nil {
		t.Fatal(err)
	}

	p2, err := peer.IDB58Decode("QmUkUQgxXeggyaD5Ckv8ZqfW8wHBX6cYyeiyqvVZYzq5Bi")
	if err != nil {
		t.Fatal(err)
	}

	addr1, err := ma.NewMultiaddr("/ip4/1.1.1.1/tcp/9999")
	if err != nil {
		t.Fatal(err)
	}
	addrs1 := [][]byte{addr1.Bytes()}

	addr2, err := ma.NewMultiaddr("/ip4/2.2.2.2/tcp/9999")
	if err != nil {
		t.Fatal(err)
	}
	addrs2 := [][]byte{addr2.Bytes()}

	err = db.Register(p1, "foo1", addrs1, 60)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Register(p1, "foo2", addrs1, 60)
	if err != nil {
		t.Fatal(err)
	}

	count, err := db.CountRegistrations(p1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatal("registrations for p1 should be 2")
	}

	rrs, cookie, err := db.Discover("", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 2 {
		t.Fatal("should have got 2 registrations")
	}
	rr := rrs[0]
	if rr.Id != p1 {
		t.Fatal("expected p1 ID in registration")
	}
	if rr.Ns != "foo1" {
		t.Fatal("expected namespace foo1 in registration")
	}
	if !equalAddrs(rr.Addrs, addrs1) {
		t.Fatal("expected p1's addrs in registration")
	}

	rr = rrs[1]
	if rr.Id != p1 {
		t.Fatal("expected p1 ID in registration")
	}
	if rr.Ns != "foo2" {
		t.Fatal("expected namespace foo1 in registration")
	}
	if !equalAddrs(rr.Addrs, addrs1) {
		t.Fatal("expected p1's addrs in registration")
	}

	err = db.Register(p2, "foo1", addrs2, 60)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Register(p2, "foo2", addrs2, 60)
	if err != nil {
		t.Fatal(err)
	}

	count, err = db.CountRegistrations(p2)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatal("registrations for p2 should be 2")
	}

	rrs, cookie, err = db.Discover("", cookie, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 2 {
		t.Fatal("should have got 2 registrations")
	}
	rr = rrs[0]
	if rr.Id != p2 {
		t.Fatal("expected p2 ID in registration")
	}
	if rr.Ns != "foo1" {
		t.Fatal("expected namespace foo1 in registration")
	}
	if !equalAddrs(rr.Addrs, addrs2) {
		t.Fatal("expected p2's addrs in registration")
	}

	rr = rrs[1]
	if rr.Id != p2 {
		t.Fatal("expected p2 ID in registration")
	}
	if rr.Ns != "foo2" {
		t.Fatal("expected namespace foo1 in registration")
	}
	if !equalAddrs(rr.Addrs, addrs2) {
		t.Fatal("expected p2's addrs in registration")
	}

	err = db.Unregister(p2, "")
	if err != nil {
		t.Fatal(err)
	}

	count, err = db.CountRegistrations(p2)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("registrations for p2 should be 0")
	}

	rrs, _, err = db.Discover("", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 2 {
		t.Fatal("should have got 2 registrations")
	}
	rr = rrs[0]
	if rr.Id != p1 {
		t.Fatal("expected p1 ID in registration")
	}
	if rr.Ns != "foo1" {
		t.Fatal("expected namespace foo1 in registration")
	}
	if !equalAddrs(rr.Addrs, addrs1) {
		t.Fatal("expected p1's addrs in registration")
	}

	rr = rrs[1]
	if rr.Id != p1 {
		t.Fatal("expected p1 ID in registration")
	}
	if rr.Ns != "foo2" {
		t.Fatal("expected namespace foo1 in registration")
	}
	if !equalAddrs(rr.Addrs, addrs1) {
		t.Fatal("expected p1's addrs in registration")
	}

	db.Close()
}

func TestDBCleanup(t *testing.T) {
	db, err := OpenDB(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	p1, err := peer.IDB58Decode("QmVr26fY1tKyspEJBniVhqxQeEjhF78XerGiqWAwraVLQH")
	if err != nil {
		t.Fatal(err)
	}

	addr1, err := ma.NewMultiaddr("/ip4/1.1.1.1/tcp/9999")
	if err != nil {
		t.Fatal(err)
	}
	addrs1 := [][]byte{addr1.Bytes()}

	err = db.Register(p1, "foo1", addrs1, 1)
	if err != nil {
		t.Fatal(err)
	}

	count, err := db.CountRegistrations(p1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("registrations for p1 should be 1")
	}

	time.Sleep(2 * time.Second)

	db.cleanupExpired()

	count, err = db.CountRegistrations(p1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("registrations for p1 should be 0")
	}

	rrs, _, err := db.Discover("foo1", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 0 {
		t.Fatal("should have got 0 registrations")
	}

	db.Close()
}
