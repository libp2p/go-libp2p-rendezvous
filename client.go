package rendezvous

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	pb "github.com/libp2p/go-libp2p-rendezvous/pb"

	ggio "github.com/gogo/protobuf/io"

	"github.com/libp2p/go-libp2p-core/host"
	inet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

var (
	DiscoverAsyncInterval = 2 * time.Minute
)

type RendezvousPoint interface {
	Register(ctx context.Context, ns string, ttl int) error
	Unregister(ctx context.Context, ns string) error
	Discover(ctx context.Context, ns string, limit int, cookie []byte) ([]Registration, []byte, error)
	DiscoverAsync(ctx context.Context, ns string) (<-chan Registration, error)
}

type Registration struct {
	Peer peer.AddrInfo
	Ns   string
	Ttl  int
}

type RendezvousClient interface {
	Register(ctx context.Context, ns string, ttl int) error
	Unregister(ctx context.Context, ns string) error
	Discover(ctx context.Context, ns string, limit int, cookie []byte) ([]peer.AddrInfo, []byte, error)
	DiscoverAsync(ctx context.Context, ns string) (<-chan peer.AddrInfo, error)
}

func NewRendezvousPoint(host host.Host, p peer.ID) RendezvousPoint {
	return &rendezvousPoint{
		host: host,
		p:    p,
	}
}

type rendezvousPoint struct {
	host host.Host
	p    peer.ID
}

func NewRendezvousClient(host host.Host, rp peer.ID) RendezvousClient {
	return NewRendezvousClientWithPoint(NewRendezvousPoint(host, rp))
}

func NewRendezvousClientWithPoint(rp RendezvousPoint) RendezvousClient {
	return &rendezvousClient{rp: rp}
}

type rendezvousClient struct {
	rp RendezvousPoint
}

func (rp *rendezvousPoint) Register(ctx context.Context, ns string, ttl int) error {
	s, err := rp.host.NewStream(ctx, rp.p, RendezvousProto)
	if err != nil {
		return err
	}
	defer s.Close()

	r := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(s)

	req := newRegisterMessage(ns, peer.AddrInfo{ID: rp.host.ID(), Addrs: rp.host.Addrs()}, ttl)
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
		return RendezvousError{Status: status, Text: res.GetRegisterResponse().GetStatusText()}
	}

	return nil
}

func (rc *rendezvousClient) Register(ctx context.Context, ns string, ttl int) error {
	if ttl < 120 {
		return fmt.Errorf("registration TTL is too short")
	}

	err := rc.rp.Register(ctx, ns, ttl)
	if err != nil {
		return err
	}

	go registerRefresh(ctx, rc.rp, ns, ttl)
	return nil
}

func registerRefresh(ctx context.Context, rz RendezvousPoint, ns string, ttl int) {
	var refresh time.Duration
	errcount := 0

	for {
		if errcount > 0 {
			// do randomized exponential backoff, up to ~4 hours
			if errcount > 7 {
				errcount = 7
			}
			backoff := 2 << uint(errcount)
			refresh = 5*time.Minute + time.Duration(rand.Intn(backoff*60000))*time.Millisecond
		} else {
			refresh = time.Duration(ttl-30) * time.Second
		}

		select {
		case <-time.After(refresh):
		case <-ctx.Done():
			return
		}

		err := rz.Register(ctx, ns, ttl)
		if err != nil {
			log.Errorf("Error registering [%s]: %s", ns, err.Error())
			errcount++
		} else {
			errcount = 0
		}
	}
}

func (rp *rendezvousPoint) Unregister(ctx context.Context, ns string) error {
	s, err := rp.host.NewStream(ctx, rp.p, RendezvousProto)
	if err != nil {
		return err
	}
	defer s.Close()

	w := ggio.NewDelimitedWriter(s)
	req := newUnregisterMessage(ns, rp.host.ID())
	return w.WriteMsg(req)
}

func (rc *rendezvousClient) Unregister(ctx context.Context, ns string) error {
	return rc.rp.Unregister(ctx, ns)
}

func (rp *rendezvousPoint) Discover(ctx context.Context, ns string, limit int, cookie []byte) ([]Registration, []byte, error) {
	s, err := rp.host.NewStream(ctx, rp.p, RendezvousProto)
	if err != nil {
		return nil, nil, err
	}
	defer s.Close()

	r := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(s)

	return discoverQuery(ns, limit, cookie, r, w)
}

func discoverQuery(ns string, limit int, cookie []byte, r ggio.Reader, w ggio.Writer) ([]Registration, []byte, error) {

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

	status := res.GetDiscoverResponse().GetStatus()
	if status != pb.Message_OK {
		return nil, nil, RendezvousError{Status: status, Text: res.GetDiscoverResponse().GetStatusText()}
	}

	regs := res.GetDiscoverResponse().GetRegistrations()
	result := make([]Registration, 0, len(regs))
	for _, reg := range regs {
		pi, err := pbToPeerInfo(reg.GetPeer())
		if err != nil {
			log.Errorf("Invalid peer info: %s", err.Error())
			continue
		}
		result = append(result, Registration{Peer: pi, Ns: reg.GetNs(), Ttl: int(reg.GetTtl())})
	}

	return result, res.GetDiscoverResponse().GetCookie(), nil
}

func (rp *rendezvousPoint) DiscoverAsync(ctx context.Context, ns string) (<-chan Registration, error) {
	s, err := rp.host.NewStream(ctx, rp.p, RendezvousProto)
	if err != nil {
		return nil, err
	}

	ch := make(chan Registration)
	go discoverAsync(ctx, ns, s, ch)
	return ch, nil
}

func discoverAsync(ctx context.Context, ns string, s inet.Stream, ch chan Registration) {
	defer s.Close()
	defer close(ch)

	r := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(s)

	const batch = 200

	var (
		cookie []byte
		regs   []Registration
		err    error
	)

	for {
		regs, cookie, err = discoverQuery(ns, batch, cookie, r, w)
		if err != nil {
			// TODO robust error recovery
			//      - handle closed streams with backoff + new stream, preserving the cookie
			//      - handle E_INVALID_COOKIE errors in that case to restart the discovery
			log.Errorf("Error in discovery [%s]: %s", ns, err.Error())
			return
		}

		for _, reg := range regs {
			select {
			case ch <- reg:
			case <-ctx.Done():
				return
			}
		}

		if len(regs) < batch {
			// TODO adaptive backoff for heavily loaded rendezvous points
			select {
			case <-time.After(DiscoverAsyncInterval):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (rc *rendezvousClient) Discover(ctx context.Context, ns string, limit int, cookie []byte) ([]peer.AddrInfo, []byte, error) {
	regs, cookie, err := rc.rp.Discover(ctx, ns, limit, cookie)
	if err != nil {
		return nil, nil, err
	}

	pinfos := make([]peer.AddrInfo, len(regs))
	for i, reg := range regs {
		pinfos[i] = reg.Peer
	}

	return pinfos, cookie, nil
}

func (rc *rendezvousClient) DiscoverAsync(ctx context.Context, ns string) (<-chan peer.AddrInfo, error) {
	rch, err := rc.rp.DiscoverAsync(ctx, ns)
	if err != nil {
		return nil, err
	}

	ch := make(chan peer.AddrInfo)
	go discoverPeersAsync(ctx, rch, ch)
	return ch, nil
}

func discoverPeersAsync(ctx context.Context, rch <-chan Registration, ch chan peer.AddrInfo) {
	defer close(ch)
	for {
		select {
		case reg, ok := <-rch:
			if !ok {
				return
			}

			select {
			case ch <- reg.Peer:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
