package test_utils

import (
	"context"
	"testing"

	bhost "github.com/libp2p/go-libp2p-blankhost"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	testutil "github.com/libp2p/go-libp2p-swarm/testing"
)

func GetRendezvousHosts(t *testing.T, ctx context.Context, n int) []host.Host {
	hosts := GetNetHosts(t, ctx, n)
	for i := 1; i < len(hosts); i++ {
		Connect(t, hosts[0], hosts[i])
	}
	return hosts
}

func GetNetHosts(t *testing.T, ctx context.Context, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		netw := testutil.GenSwarm(t)
		h := bhost.NewBlankHost(netw)
		out = append(out, h)
	}

	return out
}

func Connect(t *testing.T, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

func CheckPeerInfo(t *testing.T, pi peer.AddrInfo, host host.Host, fatal bool) bool {
	if pi.ID != host.ID() {
		if fatal {
			t.Fatal("bad registration: peer ID doesn't match host ID")
		}
		return false
	}
	addrs := host.Addrs()
	raddrs := pi.Addrs
	if len(addrs) != len(raddrs) {
		if fatal {
			t.Fatal("bad registration: peer address length mismatch")
		}
		return false
	}
	for i, addr := range addrs {
		raddr := raddrs[i]
		if !addr.Equal(raddr) {
			if fatal {
				t.Fatal("bad registration: peer address mismatch")
			}
			return false
		}
	}

	return true
}
