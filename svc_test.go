package rendezvous

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	db "github.com/libp2p/go-libp2p-rendezvous/db/sqlite"
	pb "github.com/libp2p/go-libp2p-rendezvous/pb"

	ggio "github.com/gogo/protobuf/io"
	bhost "github.com/libp2p/go-libp2p-blankhost"

	"github.com/libp2p/go-libp2p-core/host"
	inet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	testutil "github.com/libp2p/go-libp2p-swarm/testing"
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
		netw := testutil.GenSwarm(t, ctx)
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

func getRendezvousPoints(t *testing.T, hosts []host.Host) []RendezvousPoint {
	clients := make([]RendezvousPoint, len(hosts)-1)
	for i, host := range hosts[1:] {
		clients[i] = NewRendezvousPoint(host, hosts[0].ID())
	}
	return clients
}

func makeRendezvousService(ctx context.Context, host host.Host, path string) (*RendezvousService, error) {
	dbi, err := db.OpenDB(ctx, path)
	if err != nil {
		return nil, err
	}

	return NewRendezvousService(host, dbi), nil
}

func TestSVCRegistrationAndDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getRendezvousHosts(t, ctx, 5)

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.DB.Close()

	clients := getRendezvousPoints(t, hosts)

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

func TestSVCErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getRendezvousHosts(t, ctx, 2)

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.DB.Close()

	// testable registration errors
	res, err := doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("", peer.AddrInfo{}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRegisterResponse().GetStatus() != pb.Message_E_INVALID_NAMESPACE {
		t.Fatal("expected E_INVALID_NAMESPACE")
	}

	badns := make([]byte, 2*MaxNamespaceLength)
	rand.Read(badns)
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage(string(badns), peer.AddrInfo{}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRegisterResponse().GetStatus() != pb.Message_E_INVALID_NAMESPACE {
		t.Fatal("expected E_INVALID_NAMESPACE")
	}

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRegisterResponse().GetStatus() != pb.Message_E_INVALID_PEER_INFO {
		t.Fatal("expected E_INVALID_PEER_INFO")
	}

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: peer.ID("blah")}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRegisterResponse().GetStatus() != pb.Message_E_INVALID_PEER_INFO {
		t.Fatal("expected E_INVALID_PEER_INFO")
	}

	p, err := peer.IDB58Decode("QmVr26fY1tKyspEJBniVhqxQeEjhF78XerGiqWAwraVLQH")
	if err != nil {
		t.Fatal(err)
	}

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: p}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRegisterResponse().GetStatus() != pb.Message_E_INVALID_PEER_INFO {
		t.Fatal("expected E_INVALID_PEER_INFO")
	}

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: hosts[1].ID()}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRegisterResponse().GetStatus() != pb.Message_E_INVALID_PEER_INFO {
		t.Fatal("expected E_INVALID_PEER_INFO")
	}

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: hosts[1].ID(), Addrs: hosts[1].Addrs()}, 2*MaxTTL))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRegisterResponse().GetStatus() != pb.Message_E_INVALID_TTL {
		t.Fatal("expected E_INVALID_TTL")
	}

	// do MaxRegistrations
	for i := 0; i < MaxRegistrations+1; i++ {
		ns := fmt.Sprintf("foo%d", i)
		res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
			newRegisterMessage(ns, peer.AddrInfo{ID: hosts[1].ID(), Addrs: hosts[1].Addrs()}, 0))
		if err != nil {
			t.Fatal(err)
		}
		if res.GetRegisterResponse().GetStatus() != pb.Message_OK {
			t.Fatal("expected OK")
		}
	}
	// and now fail
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: hosts[1].ID(), Addrs: hosts[1].Addrs()}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRegisterResponse().GetStatus() != pb.Message_E_NOT_AUTHORIZED {
		t.Fatal("expected E_NOT_AUTHORIZED")
	}

	// testable discovery errors
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newDiscoverMessage(string(badns), 0, nil))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetDiscoverResponse().GetStatus() != pb.Message_E_INVALID_NAMESPACE {
		t.Fatal("expected E_INVALID_NAMESPACE")
	}

	badcookie := make([]byte, 10)
	rand.Read(badcookie)
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newDiscoverMessage("foo", 0, badcookie))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetDiscoverResponse().GetStatus() != pb.Message_E_INVALID_COOKIE {
		t.Fatal("expected E_INVALID_COOKIE")
	}

	badcookie = make([]byte, 40)
	rand.Read(badcookie)
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newDiscoverMessage("foo", 0, badcookie))
	if err != nil {
		t.Fatal(err)
	}
	if res.GetDiscoverResponse().GetStatus() != pb.Message_E_INVALID_COOKIE {
		t.Fatal("expected E_INVALID_COOKIE")
	}

}

func doTestRequest(ctx context.Context, host host.Host, rp peer.ID, m *pb.Message) (*pb.Message, error) {
	s, err := host.NewStream(ctx, rp, RendezvousProto)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	r := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(s)

	err = w.WriteMsg(m)
	if err != nil {
		return nil, err
	}

	res := new(pb.Message)
	err = r.ReadMsg(res)
	if err != nil {
		return nil, err
	}

	return res, nil
}
