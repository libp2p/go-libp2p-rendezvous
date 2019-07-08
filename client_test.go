package rendezvous

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

func getRendezvousClients(t *testing.T, hosts []host.Host) []RendezvousClient {
	clients := make([]RendezvousClient, len(hosts)-1)
	for i, host := range hosts[1:] {
		clients[i] = NewRendezvousClient(host, hosts[0].ID())
	}
	return clients
}

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

	recordTTL, err := clients[0].Register(ctx, "foo1", DefaultTTL)
	if err != nil {
		t.Fatal(err)
	}
	if recordTTL != DefaultTTL*time.Second {
		t.Fatalf("Expected record TTL to be %d seconds", DefaultTTL)
	}

	pi, cookie, err := clients[0].Discover(ctx, "foo1", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pi) != 1 {
		t.Fatal("Expected 1 peer")
	}
	checkPeerInfo(t, pi[0], hosts[1])

	for i, client := range clients[1:] {
		recordTTL, err = client.Register(ctx, "foo1", DefaultTTL)
		if err != nil {
			t.Fatal(err)
		}
		if recordTTL != DefaultTTL*time.Second {
			t.Fatalf("Expected record TTL to be %d seconds", DefaultTTL)
		}

		pi, cookie, err = clients[0].Discover(ctx, "foo1", 10, cookie)
		if err != nil {
			t.Fatal(err)
		}
		if len(pi) != 1 {
			t.Fatal("Expected 1 peer")
		}
		checkPeerInfo(t, pi[0], hosts[2+i])
	}

	for _, client := range clients[1:] {
		pi, _, err = client.Discover(ctx, "foo1", 10, nil)
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

	ch, err := clients[0].DiscoverAsync(ctx, "foo1")
	if err != nil {
		t.Fatal(err)
	}

	for i, client := range clients[0:] {
		recordTTL, err := client.Register(ctx, "foo1", DefaultTTL)
		if err != nil {
			t.Fatal(err)
		}
		if recordTTL != DefaultTTL*time.Second {
			t.Fatalf("Expected record TTL to be %d seconds", DefaultTTL)
		}

		pi := <-ch
		checkPeerInfo(t, pi, hosts[1+i])
	}

	DiscoverAsyncInterval = 2 * time.Minute
}

func checkPeerInfo(t *testing.T, pi peer.AddrInfo, host host.Host) {
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
