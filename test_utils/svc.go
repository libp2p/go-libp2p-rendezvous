package test_utils

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func GetRendezvousHosts(t *testing.T, ctx context.Context, m mocknet.Mocknet, n int) []host.Host {
	hosts := GetNetHosts(t, ctx, m, n)
	for i := 1; i < len(hosts); i++ {
		Connect(t, hosts[0], hosts[i])
	}
	return hosts
}

func GetNetHosts(t *testing.T, ctx context.Context, m mocknet.Mocknet, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		h, err := m.GenPeer()
		require.NoError(t, err)
		out = append(out, h)
	}

	err := m.LinkAll()
	require.NoError(t, err)
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
