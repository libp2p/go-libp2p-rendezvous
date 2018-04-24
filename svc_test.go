package rendezvous

import (
	"context"
	"testing"

	bhost "github.com/libp2p/go-libp2p-blankhost"
	host "github.com/libp2p/go-libp2p-host"
	netutil "github.com/libp2p/go-libp2p-netutil"
)

func getRendezvousHosts(t *testing.T, ctx context.Context, n int) []host.Host {
	hosts := getNetHosts(t, ctx, n)
	for i := 1; i < len(hosts); i++ {
		connect(t, hosts[0], hosts[i])
	}
	return hosts
}

func getNetHosts(t *testing.T, ctx context.Context, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		netw := netutil.GenSwarmNetwork(t, ctx)
		h := bhost.NewBlankHost(netw)
		out = append(out, h)
	}

	return out
}

func connect(t *testing.T, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

func getRendezvousClients(t *testing.T, hosts []host.Host) []Rendezvous {
	clients := make([]Rendezvous, len(hosts)-1)
	for i, host := range hosts[1:] {
		clients[i] = NewRendezvousClient(host, hosts[0].ID())
	}
	return clients
}

func TestSVCRegistrationAndDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getRendezvousHosts(t, ctx, 5)

	svc, err := NewRendezvousService(ctx, hosts[0], ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.DB.Close()

	clients := getRendezvousClients(t, hosts)

	err = clients[0].Register(ctx, "foo1", 60)
	if err != nil {
		t.Fatal(err)
	}

	rrs, cookie, err := clients[0].Discover(ctx, "foo1", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rrs) != 1 {
		t.Fatal("Expected 1 registration")
	}
	checkHostRegistration(t, rrs[0], hosts[1])

	for i, client := range clients[1:] {
		err = client.Register(ctx, "foo1", 60)
		if err != nil {
			t.Fatal(err)
		}

		rrs, cookie, err = clients[0].Discover(ctx, "foo1", 10, cookie)
		if err != nil {
			t.Fatal(err)
		}
		if len(rrs) != 1 {
			t.Fatal("Expected 1 registration")
		}
		checkHostRegistration(t, rrs[0], hosts[2+i])
	}

	for _, client := range clients[1:] {
		rrs, _, err = client.Discover(ctx, "foo1", 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rrs) != 4 {
			t.Fatal("Expected 4 registrations")
		}

		for j, rr := range rrs {
			checkHostRegistration(t, rr, hosts[1+j])
		}
	}

	err = clients[0].Unregister(ctx, "foo1")
	if err != nil {
		t.Fatal(err)
	}

	for _, client := range clients[0:] {
		rrs, _, err = client.Discover(ctx, "foo1", 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rrs) != 3 {
			t.Fatal("Expected 3 registrations")
		}

		for j, rr := range rrs {
			checkHostRegistration(t, rr, hosts[2+j])
		}
	}

	err = clients[1].Unregister(ctx, "")
	for _, client := range clients[0:] {
		rrs, _, err = client.Discover(ctx, "foo1", 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rrs) != 2 {
			t.Fatal("Expected 2 registrations")
		}

		for j, rr := range rrs {
			checkHostRegistration(t, rr, hosts[3+j])
		}
	}
}

func checkHostRegistration(t *testing.T, rr Registration, host host.Host) {
	if rr.Peer.ID != host.ID() {
		t.Fatal("bad registration: peer ID doesn't match host ID")
	}
	addrs := host.Addrs()
	raddrs := rr.Peer.Addrs
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
