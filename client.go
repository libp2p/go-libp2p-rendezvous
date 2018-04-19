package rendezvous

import (
	"context"
	"fmt"
	"time"

	pb "github.com/libp2p/go-libp2p-rendezvous/pb"

	ggio "github.com/gogo/protobuf/io"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

var log = logging.Logger("rendezvous")

type Rendezvous interface {
	Register(ctx context.Context, ns string, ttl int) error
	Unregister(ctx context.Context, ns string) error
	DiscoverOnce(ctx context.Context, ns string, limit int) ([]pstore.PeerInfo, error)
	Discover(ctx context.Context, ns string) (<-chan pstore.PeerInfo, error)
}

func NewRendezvousClient(host host.Host, rp peer.ID) Rendezvous {
	return &client{
		host: host,
		rp:   rp,
	}
}

type client struct {
	host host.Host
	rp   peer.ID
}

func (cli *client) Register(ctx context.Context, ns string, ttl int) error {
	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return err
	}
	defer s.Close()

	r := ggio.NewDelimitedReader(s, 1<<20)
	w := ggio.NewDelimitedWriter(s)

	req := newRegisterMessage(ns, pstore.PeerInfo{ID: cli.host.ID(), Addrs: cli.host.Addrs()}, ttl)
	err = w.WriteMsg(req)
	if err != nil {
		return err
	}

	var res pb.Message
	err = r.ReadMsg(&res)
	if err != nil {
		return err
	}

	if res.GetType() != pb.Message_REGISTER_RESPONSE {
		return fmt.Errorf("Unexpected response: %s", res.GetType().String())
	}

	status := res.GetRegisterResponse().GetStatus()
	if status != pb.Message_OK {
		return fmt.Errorf("Registration failure: %s", status.String())
	}

	return nil
}

func (cli *client) Unregister(ctx context.Context, ns string) error {
	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return err
	}
	defer s.Close()

	w := ggio.NewDelimitedWriter(s)
	req := newUnregisterMessage(ns, cli.host.ID())
	return w.WriteMsg(req)
}

func (cli *client) DiscoverOnce(ctx context.Context, ns string, limit int) ([]pstore.PeerInfo, error) {
	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	r := ggio.NewDelimitedReader(s, 1<<20)
	w := ggio.NewDelimitedWriter(s)

	req := newDiscoverMessage(ns, limit)
	err = w.WriteMsg(req)
	if err != nil {
		return nil, err
	}

	var res pb.Message
	err = r.ReadMsg(&res)
	if err != nil {
		return nil, err
	}

	if res.GetType() != pb.Message_DISCOVER_RESPONSE {
		return nil, fmt.Errorf("Unexpected response: %s", res.GetType().String())
	}

	regs := res.GetDiscoverResponse().GetRegistrations()
	pinfos := make([]pstore.PeerInfo, 0, len(regs))
	for _, reg := range regs {
		pi, err := pbToPeerInfo(reg.GetPeer())
		if err != nil {
			log.Errorf("Invalid peer info: %s", err.Error())
			continue
		}
		pinfos = append(pinfos, pi)
	}

	return pinfos, nil
}

func (cli *client) Discover(ctx context.Context, ns string) (<-chan pstore.PeerInfo, error) {
	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return nil, err
	}

	ch := make(chan pstore.PeerInfo)
	go doDiscovery(ctx, ns, s, ch)
	return ch, nil
}

func doDiscovery(ctx context.Context, ns string, s inet.Stream, ch chan pstore.PeerInfo) {
	defer s.Close()
	defer close(ch)

	const batch = 100

	r := ggio.NewDelimitedReader(s, 1<<20)
	w := ggio.NewDelimitedWriter(s)

	req := newDiscoverMessage(ns, batch)

	for {
		err := w.WriteMsg(req)
		if err != nil {
			log.Errorf("Error sending Discover request: %s", err.Error())
			return
		}

		var res pb.Message
		err = r.ReadMsg(&res)
		if err != nil {
			log.Errorf("Error reading discover response: %s", err.Error())
			return
		}

		if res.GetType() != pb.Message_DISCOVER_RESPONSE {
			log.Errorf("Unexpected response: %s", res.GetType().String())
			return
		}

		regs := res.GetDiscoverResponse().GetRegistrations()
		for _, reg := range regs {
			pinfo, err := pbToPeerInfo(reg.GetPeer())
			if err != nil {
				log.Errorf("Invalid peer info: %s", err.Error())
				continue
			}

			select {
			case ch <- pinfo:
			case <-ctx.Done():
				return
			}
		}

		req.Discover.Cookie = res.GetDiscoverResponse().GetCookie()

		if len(regs) < batch {
			select {
			case <-time.After(1 * time.Minute):
			case <-ctx.Done():
				return
			}
		}
	}
}
