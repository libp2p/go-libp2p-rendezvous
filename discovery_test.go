package rendezvous

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

func getRendezvousDiscovery(hosts []host.Host) []discovery.Discovery {
	clients := make([]discovery.Discovery, len(hosts)-1)
	rendezvousPeer := hosts[0].ID()
	for i, h := range hosts[1:] {
		rp := NewRendezvousPoint(h, rendezvousPeer)
		rng := rand.New(rand.NewSource(int64(i)))
		clients[i] = &rendezvousDiscovery{rp: rp, peerCache: make(map[string]*discoveryCache), rng: rng}
	}
	return clients
}

func peerChannelToArray(pch <-chan peer.AddrInfo) []peer.AddrInfo {
	pi := make([]peer.AddrInfo, len(pch))
	peerIndex := 0
	for p := range pch {
		pi[peerIndex] = p
		peerIndex++
	}
	return pi
}

func checkAvailablePeers(t *testing.T, ctx context.Context, client discovery.Discovery, namespace string, expectedNumPeers int) {
	pch, err := client.FindPeers(ctx, namespace)
	if err != nil {
		t.Fatal(err)
	}

	pi := peerChannelToArray(pch)

	if len(pi) != expectedNumPeers {
		t.Fatalf("Expected %d peers", expectedNumPeers)
	}
}

func TestDiscoveryClientAdvertiseAndFindPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	defer m.Close()

	// Define parameters
	const namespace = "foo1"
	const numClients = 4
	const ttl = DefaultTTL * time.Second

	// Instantiate server and clients
	hosts := getRendezvousHosts(t, ctx, m, numClients+1)

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.DB.Close()

	clients := getRendezvousDiscovery(hosts)

	// Advertise and check one peer
	_, err = clients[0].Advertise(ctx, namespace, discovery.TTL(ttl))
	if err != nil {
		t.Fatal(err)
	}

	checkAvailablePeers(t, ctx, clients[0], namespace, 1)

	// Advertise and check the rest of the peers incrementally
	for i, client := range clients[1:] {
		if _, err = client.Advertise(ctx, namespace, discovery.TTL(ttl)); err != nil {
			t.Fatal(err)
		}

		checkAvailablePeers(t, ctx, client, namespace, i+2)
	}

	// Check that the first peer can get all the new records
	checkAvailablePeers(t, ctx, clients[0], namespace, numClients)
}

func TestDiscoveryClientExpiredCachedRecords(t *testing.T) {
	BaseDiscoveryClientCacheExpirationTest(t, true)
}

func TestDiscoveryClientExpiredManyCachedRecords(t *testing.T) {
	BaseDiscoveryClientCacheExpirationTest(t, false)
}

func BaseDiscoveryClientCacheExpirationTest(t *testing.T, onlyRequestFromCache bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Define parameters
	const numShortLivedRegs = 5
	const everyIthRegIsLongTTL = 2
	const numBaseRegs = numShortLivedRegs * everyIthRegIsLongTTL
	const namespace = "foo1"
	const longTTL = DefaultTTL * time.Second
	const shortTTL = 2 * time.Second

	m := mocknet.New()
	defer m.Close()

	// Instantiate server and clients
	hosts := getRendezvousHosts(t, ctx, m, numBaseRegs+3)

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.DB.Close()
	clients := getRendezvousDiscovery(hosts)

	// Advertise most clients
	for i, client := range clients[2:] {
		ttl := shortTTL
		if i%everyIthRegIsLongTTL == 0 {
			ttl = longTTL
		}

		if _, err = client.Advertise(ctx, namespace, discovery.TTL(ttl)); err != nil {
			t.Fatal(err)
		}
	}

	// Find peers from an unrelated client (results should be cached)
	pch, err := clients[0].FindPeers(ctx, namespace)
	if err != nil {
		t.Fatal(err)
	}
	pi := peerChannelToArray(pch)
	if len(pi) != numBaseRegs {
		t.Fatalf("expected %d registrations", numBaseRegs)
	}

	// Advertise from a new unrelated peer
	if _, err := clients[1].Advertise(ctx, namespace, discovery.TTL(longTTL)); err != nil {
		t.Fatal(err)
	}

	// Wait for cache expiration
	time.Sleep(shortTTL + time.Second)

	// Check if number of retrieved records matches caching expectations after expiration
	expectedNumClients := numShortLivedRegs
	if !onlyRequestFromCache {
		expectedNumClients++
	}
	pch, err = clients[0].FindPeers(ctx, namespace, discovery.Limit(expectedNumClients))
	if err != nil {
		t.Fatal(err)
	}
	pi = peerChannelToArray(pch)

	if len(pi) != expectedNumClients {
		t.Fatalf("received an incorrect number of records: %d", len(pi))
	}
}
