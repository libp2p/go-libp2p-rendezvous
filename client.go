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

const DefaultTTL = 2 * 3600 // 2hr

type Rendezvous interface {
	Register(ctx context.Context, ns string, ttl int) error
	Unregister(ctx context.Context, ns string) error
	Discover(ctx context.Context, ns string, limit int, cookie []byte) ([]pstore.PeerInfo, []byte, error)
	DiscoverAsync(ctx context.Context, ns string) (<-chan pstore.PeerInfo, error)
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
		return RegistrationError(status)
	}

	return nil
}

func Register(ctx context.Context, rz Rendezvous, ns string) error {
	err := rz.Register(ctx, ns, DefaultTTL)
	if err != nil {
		return err
	}

	go registerRefresh(ctx, rz, ns)
	return nil
}

func registerRefresh(ctx context.Context, rz Rendezvous, ns string) {
	const refresh = DefaultTTL - 30

	for {
		select {
		case <-time.After(refresh * time.Second):
		case <-ctx.Done():
			return
		}

		err := rz.Register(ctx, ns, DefaultTTL)
		if err != nil {
			log.Errorf("Error registering [%s]: %s", ns, err.Error())
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

func (cli *client) Discover(ctx context.Context, ns string, limit int, cookie []byte) ([]pstore.PeerInfo, []byte, error) {
	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return nil, nil, err
	}
	defer s.Close()

	r := ggio.NewDelimitedReader(s, 1<<20)
	w := ggio.NewDelimitedWriter(s)

	return discoverQuery(ns, limit, cookie, r, w)
}

func discoverQuery(ns string, limit int, cookie []byte, r ggio.Reader, w ggio.Writer) ([]pstore.PeerInfo, []byte, error) {

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

func (cli *client) DiscoverAsync(ctx context.Context, ns string) (<-chan pstore.PeerInfo, error) {
	s, err := cli.host.NewStream(ctx, cli.rp, RendezvousProto)
	if err != nil {
		return nil, err
	}

	ch := make(chan pstore.PeerInfo)
	go discoverAsync(ctx, ns, s, ch)
	return ch, nil
}

func discoverAsync(ctx context.Context, ns string, s inet.Stream, ch chan pstore.PeerInfo) {
	defer s.Close()
	defer close(ch)

	r := ggio.NewDelimitedReader(s, 1<<20)
	w := ggio.NewDelimitedWriter(s)

	const batch = 100

	var (
		cookie []byte
		pi     []pstore.PeerInfo
		err    error
	)

	for {
		pi, cookie, err = discoverQuery(ns, batch, cookie, r, w)
		if err != nil {
			log.Errorf("Error in discovery [%s]: %s", ns, err.Error())
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
			case <-time.After(2 * time.Minute):
			case <-ctx.Done():
				return
			}
		}
	}
}
