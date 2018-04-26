package rendezvous

import (
	"context"
	"testing"
	"time"

	host "github.com/libp2p/go-libp2p-host"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

func TestClientRegistrationAndDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getRendezvousHosts(t, ctx, 5)

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.DB.Close()

	clients := getRendezvousClients(t, hosts)

	err = Register(ctx, clients[0], "foo1", DefaultTTL)
	if err != nil {
		t.Fatal(err)
	}

	pi, cookie, err := DiscoverPeers(ctx, clients[0], "foo1", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pi) != 1 {
		t.Fatal("Expected 1 peer")
	}
	checkPeerInfo(t, pi[0], hosts[1])

	for i, client := range clients[1:] {
		err = Register(ctx, client, "foo1", DefaultTTL)
		if err != nil {
			t.Fatal(err)
		}

		pi, cookie, err = DiscoverPeers(ctx, clients[0], "foo1", 10, cookie)
		if err != nil {
			t.Fatal(err)
		}
		if len(pi) != 1 {
			t.Fatal("Expected 1 peer")
		}
		checkPeerInfo(t, pi[0], hosts[2+i])
	}

	for _, client := range clients[1:] {
		pi, _, err = DiscoverPeers(ctx, client, "foo1", 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(pi) != 4 {
			t.Fatal("Expected 4 registrations")
		}

		for j, p := range pi {
			checkPeerInfo(t, p, hosts[1+j])
		}
	}
}

func TestClientRegistrationAndDiscoveryAsync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getRendezvousHosts(t, ctx, 5)

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.DB.Close()

	clients := getRendezvousClients(t, hosts)

	DiscoverAsyncInterval = 1 * time.Second

	ch, err := DiscoverPeersAsync(ctx, clients[0], "foo1")
	if err != nil {
		t.Fatal(err)
	}

	for i, client := range clients[0:] {
		err = Register(ctx, client, "foo1", DefaultTTL)
		if err != nil {
			t.Fatal(err)
		}

		pi := <-ch
		checkPeerInfo(t, pi, hosts[1+i])
	}

	DiscoverAsyncInterval = 2 * time.Minute
}

func checkPeerInfo(t *testing.T, pi pstore.PeerInfo, host host.Host) {
	if pi.ID != host.ID() {
		t.Fatal("bad registration: peer ID doesn't match host ID")
	}
	addrs := host.Addrs()
	raddrs := pi.Addrs
	if len(addrs) != len(raddrs) {
		t.Fatal("bad registration: peer address length mismatch")
	}
	for i, addr := range addrs {
		raddr := raddrs[i]
		if !addr.Equal(raddr) {
			t.Fatal("bad registration: peer address mismatch")
		}
	}
}
