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
	RegisterOnce(ctx context.Context, ns string, ttl int) error
	Register(ctx context.Context, ns string, opts ...interface{}) error
	Unregister(ctx context.Context, ns string) error
	DiscoverOnce(ctx context.Context, ns string, limit int, cookie []byte) ([]pstore.PeerInfo, []byte, error)
	Discover(ctx context.Context, ns string, opts ...interface{}) (<-chan pstore.PeerInfo, error)
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

func (cli *client) RegisterOnce(ctx context.Context, ns string, ttl int) error {
	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return err
	}
	defer s.Close()

	return cli.registerOnce(ctx, ns, ttl, s)
}

func (cli *client) registerOnce(ctx context.Context, ns string, ttl int, s inet.Stream) error {
	r := ggio.NewDelimitedReader(s, 1<<20)
	w := ggio.NewDelimitedWriter(s)

	req := newRegisterMessage(ns, pstore.PeerInfo{ID: cli.host.ID(), Addrs: cli.host.Addrs()}, ttl)
	err := w.WriteMsg(req)
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

func (cli *client) Register(ctx context.Context, ns string, opts ...interface{}) error {
	var E func(error)

	for _, opt := range opts {
		switch o := opt.(type) {
		case func(error):
			E = o

		default:
			return fmt.Errorf("Unexpected option: %v", opt)
		}
	}

	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return err
	}

	go cli.doRegister(ctx, ns, E, s)
	return nil
}

func (cli *client) doRegister(ctx context.Context, ns string, E func(error), s inet.Stream) {
	const ttl = 2 * 3600 // 2hr
	const refresh = ttl - 30

	defer s.Close()
	for {
		err := cli.registerOnce(ctx, ns, ttl, s)
		if err != nil {
			log.Errorf("Error registering [%s]: %s", ns, err.Error())
			if E != nil {
				go E(err)
			}
			return
		}

		select {
		case <-time.After(refresh * time.Second):
		case <-ctx.Done():
			return
		}
	}
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

func (cli *client) DiscoverOnce(ctx context.Context, ns string, limit int, cookie []byte) ([]pstore.PeerInfo, []byte, error) {
	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return nil, nil, err
	}
	defer s.Close()

	return cli.discoverOnce(ctx, ns, limit, cookie, s)
}

func (cli *client) discoverOnce(ctx context.Context, ns string, limit int, cookie []byte, s inet.Stream) ([]pstore.PeerInfo, []byte, error) {
	r := ggio.NewDelimitedReader(s, 1<<20)
	w := ggio.NewDelimitedWriter(s)

	req := newDiscoverMessage(ns, limit, cookie)
	err := w.WriteMsg(req)
	if err != nil {
		return nil, nil, err
	}

	var res pb.Message
	err = r.ReadMsg(&res)
	if err != nil {
		return nil, nil, err
	}

	if res.GetType() != pb.Message_DISCOVER_RESPONSE {
		return nil, nil, fmt.Errorf("Unexpected response: %s", res.GetType().String())
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

	return pinfos, res.GetDiscoverResponse().GetCookie(), nil
}

func (cli *client) Discover(ctx context.Context, ns string, opts ...interface{}) (<-chan pstore.PeerInfo, error) {
	var E func(error)

	for _, opt := range opts {
		switch o := opt.(type) {
		case func(error):
			E = o

		default:
			return nil, fmt.Errorf("Unexpected option: %v", opt)
		}
	}

	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return nil, err
	}

	ch := make(chan pstore.PeerInfo)
	go cli.doDiscover(ctx, ns, E, s, ch)
	return ch, nil
}

func (cli *client) doDiscover(ctx context.Context, ns string, E func(error), s inet.Stream, ch chan pstore.PeerInfo) {
	defer s.Close()
	defer close(ch)

	const batch = 100

	var (
		cookie []byte
		pi     []pstore.PeerInfo
		err    error
	)
	for {
		pi, cookie, err = cli.discoverOnce(ctx, ns, batch, cookie, s)
		if err != nil {
			log.Errorf("Error in discovery [%s]: %s", ns, err.Error())
			if E != nil {
				go E(err)
			}
			return
		}

		for _, p := range pi {
			select {
			case ch <- p:
			case <-ctx.Done():
				return
			}
		}

		if len(pi) < batch {
			select {
			case <-time.After(1 * time.Minute):
			case <-ctx.Done():
				return
			}
		}
	}
}
