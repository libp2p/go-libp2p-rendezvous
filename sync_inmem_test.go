package rendezvous_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rendezvous "github.com/libp2p/go-libp2p-rendezvous"
	db "github.com/libp2p/go-libp2p-rendezvous/db/sqlite"
	"github.com/libp2p/go-libp2p-rendezvous/test_utils"
	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

func makeRendezvousService(ctx context.Context, host host.Host, path string, rzs ...rendezvous.RendezvousSync) (*rendezvous.RendezvousService, error) {
	dbi, err := db.OpenDB(ctx, path)
	if err != nil {
		return nil, err
	}

	return rendezvous.NewRendezvousService(host, dbi, rzs...), nil
}

func getRendezvousClients(ctx context.Context, t *testing.T, hosts []host.Host) []rendezvous.RendezvousClient {
	t.Helper()

	clients := make([]rendezvous.RendezvousClient, len(hosts)-1)
	for i, host := range hosts[1:] {
		syncClient := rendezvous.NewSyncInMemClient(ctx, host)
		clients[i] = rendezvous.NewRendezvousClient(host, hosts[0].ID(), syncClient)
	}
	return clients
}

func TestFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mocknet.New()
	defer m.Close()

	// Instantiate server and clients
	hosts := test_utils.GetRendezvousHosts(t, ctx, m, 4)

	inmemPubSubSync, err := rendezvous.NewSyncInMemProvider(hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	svc, err := makeRendezvousService(ctx, hosts[0], ":memory:", inmemPubSubSync)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.DB.Close()

	clients := getRendezvousClients(ctx, t, hosts)

	regFound := int64(0)
	wg := sync.WaitGroup{}

	const announcementCount = 5

	for _, client := range clients[1:] {
		wg.Add(1)
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		ch, err := client.DiscoverSubscribe(ctx, "foo1")
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			regFoundForPeer := 0

			defer cancel()
			defer wg.Done()

			for p := range ch {
				if test_utils.CheckPeerInfo(t, p, hosts[2], false) == true {
					regFoundForPeer++
					atomic.AddInt64(&regFound, 1)
				}

				if regFoundForPeer == announcementCount {
					go func() {
						// this allows more events to be received
						time.Sleep(time.Millisecond * 500)
						cancel()
					}()
				}
			}
		}()
	}

	for i := 0; i < announcementCount; i++ {
		_, err = clients[1].Register(ctx, "foo1", rendezvous.DefaultTTL)
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()
	if regFound != int64(len(clients[1:]))*announcementCount {
		t.Fatalf("expected %d records to be found got %d", int64(len(clients[1:])), regFound)
	}
}
