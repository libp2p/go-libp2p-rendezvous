package rendezvous

import (
	"fmt"

	db "github.com/libp2p/go-libp2p-rendezvous/db"
	pb "github.com/libp2p/go-libp2p-rendezvous/pb"

	ggio "github.com/gogo/protobuf/io"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
)

const (
	MaxTTL               = 72 * 3600 // 72hr
	MaxNamespaceLength   = 256
	MaxPeerAddressLength = 2048
	MaxRegistrations     = 1000
	MaxDiscoverLimit     = 1000
)

type RendezvousService struct {
	DB  db.DB
	rzs []RendezvousSync
}

type RendezvousSync interface {
	Register(p peer.ID, ns string, addrs [][]byte, ttl int)
	Unregister(p peer.ID, ns string)
}

func NewRendezvousService(host host.Host, db db.DB, rzs ...RendezvousSync) *RendezvousService {
	rz := &RendezvousService{DB: db, rzs: rzs}
	host.SetStreamHandler(RendezvousProto, rz.handleStream)
	return rz
}

func (rz *RendezvousService) handleStream(s inet.Stream) {
	pid := s.Conn().RemotePeer()
	log.Debugf("New stream from %s", pid.Pretty())

	r := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(s)

	for {
		var req pb.Message
		var res pb.Message

		err := r.ReadMsg(&req)
		if err != nil {
			s.Reset()
			return
		}

		t := req.GetType()
		switch t {
		case pb.Message_REGISTER:
			r := rz.handleRegister(pid, req.GetRegister())
			res.Type = pb.Message_REGISTER_RESPONSE.Enum()
			res.RegisterResponse = r
			err = w.WriteMsg(&res)
			if err != nil {
				log.Debugf("Error writing response: %s", err.Error())
				s.Reset()
				return
			}

		case pb.Message_UNREGISTER:
			err := rz.handleUnregister(pid, req.GetUnregister())
			if err != nil {
				log.Debugf("Error unregistering peer: %s", err.Error())
			}

		case pb.Message_DISCOVER:
			r := rz.handleDiscover(pid, req.GetDiscover())
			res.Type = pb.Message_DISCOVER_RESPONSE.Enum()
			res.DiscoverResponse = r
			err = w.WriteMsg(&res)
			if err != nil {
				log.Debugf("Error writing response: %s", err.Error())
				s.Reset()
				return
			}

		default:
			log.Debugf("Unexpected message: %s", t.String())
			s.Reset()
			return
		}
	}
}

func (rz *RendezvousService) handleRegister(p peer.ID, m *pb.Message_Register) *pb.Message_RegisterResponse {
	ns := m.GetNs()
	if ns == "" {
		return newRegisterResponseError(pb.Message_E_INVALID_NAMESPACE, "unspecified namespace")
	}

	if len(ns) > MaxNamespaceLength {
		return newRegisterResponseError(pb.Message_E_INVALID_NAMESPACE, "namespace too long")
	}

	mpi := m.GetPeer()
	if mpi == nil {
		return newRegisterResponseError(pb.Message_E_INVALID_PEER_INFO, "missing peer info")
	}

	mpid := mpi.GetId()
	if mpid != nil {
		mp, err := peer.IDFromBytes(mpid)
		if err != nil {
			return newRegisterResponseError(pb.Message_E_INVALID_PEER_INFO, "bad peer id")
		}

		if mp != p {
			return newRegisterResponseError(pb.Message_E_INVALID_PEER_INFO, "peer id mismatch")
		}
	}

	maddrs := mpi.GetAddrs()
	if len(maddrs) == 0 {
		return newRegisterResponseError(pb.Message_E_INVALID_PEER_INFO, "missing peer addresses")
	}

	mlen := 0
	for _, maddr := range maddrs {
		mlen += len(maddr)
	}
	if mlen > MaxPeerAddressLength {
		return newRegisterResponseError(pb.Message_E_INVALID_PEER_INFO, "peer info too long")
	}

	// Note:
	// We don't validate the addresses, because they could include protocols we don't understand
	// Perhaps we should though.

	mttl := m.GetTtl()
	if mttl < 0 || mttl > MaxTTL {
		return newRegisterResponseError(pb.Message_E_INVALID_TTL, "bad ttl")
	}

	ttl := DefaultTTL
	if mttl > 0 {
		ttl = int(mttl)
	}

	// now check how many registrations we have for this peer -- simple limit to defend
	// against trivial DoS attacks (eg a peer connects and keeps registering until it
	// fills our db)
	rcount, err := rz.DB.CountRegistrations(p)
	if err != nil {
		log.Errorf("Error counting registrations: %s", err.Error())
		return newRegisterResponseError(pb.Message_E_INTERNAL_ERROR, err.Error())
	}

	if rcount > MaxRegistrations {
		log.Warningf("Too many registrations for %s", p)
		return newRegisterResponseError(pb.Message_E_NOT_AUTHORIZED, "too many registrations")
	}

	// ok, seems like we can register
	err = rz.DB.Register(p, ns, maddrs, ttl)
	if err != nil {
		log.Errorf("Error registering: %s", err.Error())
		return newRegisterResponseError(pb.Message_E_INTERNAL_ERROR, err.Error())
	}

	log.Infof("registered peer %s %s (%d)", p, ns, ttl)

	for _, rzs := range rz.rzs {
		rzs.Register(p, ns, maddrs, ttl)
	}

	return newRegisterResponse()
}

func (rz *RendezvousService) handleUnregister(p peer.ID, m *pb.Message_Unregister) error {
	ns := m.GetNs()

	mpid := m.GetId()
	if mpid != nil {
		mp, err := peer.IDFromBytes(mpid)
		if err != nil {
			return err
		}

		if mp != p {
			return fmt.Errorf("peer id mismatch: %s asked to unregister %s", p.Pretty(), mp.Pretty())
		}
	}

	err := rz.DB.Unregister(p, ns)
	if err != nil {
		return err
	}

	log.Infof("unregistered peer %s %s", p, ns)

	for _, rzs := range rz.rzs {
		rzs.Unregister(p, ns)
	}

	return nil
}

func (rz *RendezvousService) handleDiscover(p peer.ID, m *pb.Message_Discover) *pb.Message_DiscoverResponse {
	ns := m.GetNs()

	if len(ns) > MaxNamespaceLength {
		return newDiscoverResponseError(pb.Message_E_INVALID_NAMESPACE, "namespace too long")
	}

	limit := MaxDiscoverLimit
	mlimit := m.GetLimit()
	if mlimit > 0 && mlimit < int64(limit) {
		limit = int(mlimit)
	}

	cookie := m.GetCookie()
	if cookie != nil && !rz.DB.ValidCookie(ns, cookie) {
		return newDiscoverResponseError(pb.Message_E_INVALID_COOKIE, "bad cookie")
	}

	regs, cookie, err := rz.DB.Discover(ns, cookie, limit)
	if err != nil {
		log.Errorf("Error in query: %s", err.Error())
		return newDiscoverResponseError(pb.Message_E_INTERNAL_ERROR, err.Error())
	}

	log.Infof("discover query: %s %s -> %d", p, ns, len(regs))

	return newDiscoverResponse(regs, cookie)
}
