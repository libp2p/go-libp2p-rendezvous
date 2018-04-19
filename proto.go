package rendezvous

import (
	pb "github.com/libp2p/go-libp2p-rendezvous/pb"

	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	protocol "github.com/libp2p/go-libp2p-protocol"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	RendezvousProto = protocol.ID("/rendezvous/1.0.0")
)

func newRegisterMessage(ns string, pi pstore.PeerInfo, ttl int) *pb.Message {
	msg := new(pb.Message)
	msg.Type = pb.Message_REGISTER.Enum()
	msg.Register = new(pb.Message_Register)
	if ns != "" {
		msg.Register.Ns = &ns
	}
	if ttl > 0 {
		ttl64 := int64(ttl)
		msg.Register.Ttl = &ttl64
	}
	msg.Register.Peer = new(pb.Message_PeerInfo)
	msg.Register.Peer.Id = []byte(pi.ID)
	msg.Register.Peer.Addrs = make([][]byte, len(pi.Addrs))
	for i, addr := range pi.Addrs {
		msg.Register.Peer.Addrs[i] = addr.Bytes()
	}
	return msg
}

func newUnregisterMessage(ns string, pid peer.ID) *pb.Message {
	msg := new(pb.Message)
	msg.Type = pb.Message_UNREGISTER.Enum()
	msg.Unregister = new(pb.Message_Unregister)
	if ns != "" {
		msg.Unregister.Ns = &ns
	}
	msg.Unregister.Id = []byte(pid)
	return msg
}

func newDiscoverMessage(ns string, limit int) *pb.Message {
	msg := new(pb.Message)
	msg.Type = pb.Message_DISCOVER.Enum()
	msg.Discover = new(pb.Message_Discover)
	if ns != "" {
		msg.Discover.Ns = &ns
	}
	if limit > 0 {
		limit64 := int64(limit)
		msg.Discover.Limit = &limit64
	}
	return msg
}

func pbToPeerInfo(p *pb.Message_PeerInfo) (pstore.PeerInfo, error) {
	id, err := peer.IDFromBytes(p.Id)
	if err != nil {
		return pstore.PeerInfo{}, err
	}
	addrs := make([]ma.Multiaddr, 0, len(p.Addrs))
	for _, bs := range p.Addrs {
		addr, err := ma.NewMultiaddrBytes(bs)
		if err != nil {
			log.Errorf("Error parsing multiaddr: %s", err.Error())
			continue
		}
		addrs = append(addrs, addr)
	}

	return pstore.PeerInfo{ID: id, Addrs: addrs}, nil
}
