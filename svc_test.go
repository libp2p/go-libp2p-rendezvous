package rendezvous

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	ggio "github.com/gogo/protobuf/io"
	"github.com/libp2p/go-libp2p/core/host"
	inet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	db "github.com/libp2p/go-libp2p-rendezvous/db/sqlite"
	pb "github.com/libp2p/go-libp2p-rendezvous/pb"
	"github.com/libp2p/go-libp2p-rendezvous/test_utils"
)

func getRendezvousHosts(t *testing.T, ctx context.Context, m mocknet.Mocknet, n int) []host.Host {
	return test_utils.GetRendezvousHosts(t, ctx, m, n)
}

func makeRendezvousService(ctx context.Context, host host.Host, path string) (*RendezvousService, error) {
	dbi, err := db.OpenDB(ctx, path)
	if err != nil {
		return nil, err
	}

	return NewRendezvousService(host, dbi), nil
}

func getRendezvousPointsTest(t *testing.T, hosts []host.Host) []RendezvousPoint {
	clients := make([]RendezvousPoint, len(hosts)-1)
	for i, host := range hosts[1:] {
		clients[i] = NewRendezvousPoint(host, hosts[0].ID())
	}
	return clients
}

func TestSVCRegistrationAndDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	defer m.Close()

	hosts := getRendezvousHosts(t, ctx, m, 5)

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:")
	require.NoError(t, err)
	defer svc.DB.Close()

	clients := getRendezvousPointsTest(t, hosts)

	const registerTTL = 60
	recordTTL, err := clients[0].Register(ctx, "foo1", registerTTL)
	require.NoError(t, err)
	require.Equalf(t, registerTTL*time.Second, recordTTL, "expected record TTL to be %d seconds", DefaultTTL)

	rrs, cookie, err := clients[0].Discover(ctx, "foo1", 10, nil)
	require.NoError(t, err)
	require.Len(t, rrs, 1)
	checkHostRegistration(t, rrs[0], hosts[1])

	for i, client := range clients[1:] {
		recordTTL, err = client.Register(ctx, "foo1", registerTTL)
		require.NoError(t, err)
		require.Equalf(t, registerTTL*time.Second, recordTTL, "expected record TTL to be %d seconds", DefaultTTL)

		rrs, cookie, err = clients[0].Discover(ctx, "foo1", 10, cookie)
		require.NoError(t, err)
		require.Len(t, rrs, 1)
		checkHostRegistration(t, rrs[0], hosts[2+i])
	}

	for _, client := range clients[1:] {
		rrs, _, err = client.Discover(ctx, "foo1", 10, nil)
		require.NoError(t, err)
		require.Len(t, rrs, 4)

		for j, rr := range rrs {
			checkHostRegistration(t, rr, hosts[1+j])
		}
	}

	err = clients[0].Unregister(ctx, "foo1")
	require.NoError(t, err)

	for _, client := range clients[0:] {
		rrs, _, err = client.Discover(ctx, "foo1", 10, nil)
		require.NoError(t, err)
		require.Lenf(t, rrs, 3, "Expected 3 registrations, got %d", len(rrs))

		for j, rr := range rrs {
			checkHostRegistration(t, rr, hosts[2+j])
		}
	}

	err = clients[1].Unregister(ctx, "")
	require.NoError(t, err)

	for _, client := range clients[0:] {
		rrs, _, err = client.Discover(ctx, "foo1", 10, nil)
		require.NoError(t, err)
		require.Len(t, rrs, 2)

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
	require.Equal(t, len(addrs), len(raddrs), "bad registration: peer address length mismatch")

	for i, addr := range addrs {
		raddr := raddrs[i]
		require.True(t, addr.Equal(raddr), "bad registration: peer address mismatch")
	}
}

func TestSVCErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	defer m.Close()

	hosts := getRendezvousHosts(t, ctx, m, 2)

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:")
	require.NoError(t, err)
	defer svc.DB.Close()

	// testable registration errors
	res, err := doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("", peer.AddrInfo{}, 0))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_NAMESPACE, res.GetRegisterResponse().GetStatus())

	badns := make([]byte, 2*MaxNamespaceLength)
	rand.Read(badns)
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage(string(badns), peer.AddrInfo{}, 0))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_NAMESPACE, res.GetRegisterResponse().GetStatus())

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{}, 0))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_PEER_INFO, res.GetRegisterResponse().GetStatus())

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: peer.ID("blah")}, 0))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_PEER_INFO, res.GetRegisterResponse().GetStatus())

	p, err := peer.Decode("QmVr26fY1tKyspEJBniVhqxQeEjhF78XerGiqWAwraVLQH")
	require.NoError(t, err)

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: p}, 0))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_PEER_INFO, res.GetRegisterResponse().GetStatus())

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: hosts[1].ID()}, 0))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_PEER_INFO, res.GetRegisterResponse().GetStatus())

	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newRegisterMessage("foo", peer.AddrInfo{ID: hosts[1].ID(), Addrs: hosts[1].Addrs()}, 2*MaxTTL))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_TTL, res.GetRegisterResponse().GetStatus())

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
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_NOT_AUTHORIZED, res.GetRegisterResponse().GetStatus())

	// testable discovery errors
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newDiscoverMessage(string(badns), 0, nil))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_NAMESPACE, res.GetDiscoverResponse().GetStatus())

	badcookie := make([]byte, 10)
	rand.Read(badcookie)
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newDiscoverMessage("foo", 0, badcookie))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_COOKIE, res.GetDiscoverResponse().GetStatus())

	badcookie = make([]byte, 40)
	rand.Read(badcookie)
	res, err = doTestRequest(ctx, hosts[1], hosts[0].ID(),
		newDiscoverMessage("foo", 0, badcookie))
	require.NoError(t, err)
	require.Equal(t, pb.Message_E_INVALID_COOKIE, res.GetDiscoverResponse().GetStatus())
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
