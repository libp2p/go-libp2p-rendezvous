package rendezvous

import (
	"context"
	"github.com/libp2p/go-libp2p-core/discovery"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"math"
	"math/rand"
	"sync"
	"time"
)

type rendezvousDiscovery struct {
	rp        RendezvousPoint
	peerCache sync.Map //is a map[string]discoveredPeerCache
	rng       *rand.Rand
	rngMux    sync.Mutex
}

type discoveredPeerCache struct {
	cachedRecs map[peer.ID]*record
	cookie     []byte
	mux        sync.Mutex
}

type record struct {
	peer   peer.AddrInfo
	expire int64
}

func NewRendezvousDiscovery(host host.Host, rendezvousPeer peer.ID) discovery.Discovery {
	rp := NewRendezvousPoint(host, rendezvousPeer)
	return &rendezvousDiscovery{rp, sync.Map{}, rand.New(rand.NewSource(rand.Int63())), sync.Mutex{}}
}

func (c *rendezvousDiscovery) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	// Get options
	var options discovery.Options
	err := options.Apply(opts...)
	if err != nil {
		return 0, err
	}

	ttl := options.Ttl
	var ttlSeconds int

	if ttl == 0 {
		ttlSeconds = 7200
	} else {
		ttlSeconds = int(math.Round(ttl.Seconds()))
	}

	if returnedTTL, err := c.rp.Register(ctx, ns, ttlSeconds); err != nil {
		return 0, err
	} else {
		return returnedTTL, nil
	}
}

func (c *rendezvousDiscovery) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	// Get options
	var options discovery.Options
	err := options.Apply(opts...)
	if err != nil {
		return nil, err
	}

	const maxLimit = 1000
	limit := options.Limit
	if limit == 0 || limit > maxLimit {
		limit = maxLimit
	}

	// Get cached peers
	var cache *discoveredPeerCache

	genericCache, _ := c.peerCache.LoadOrStore(ns, &discoveredPeerCache{})
	cache = genericCache.(*discoveredPeerCache)

	cache.mux.Lock()
	defer cache.mux.Unlock()

	// Remove all expired entries from cache
	currentTime := time.Now().Unix()
	newCacheSize := len(cache.cachedRecs)

	for p := range cache.cachedRecs {
		rec := cache.cachedRecs[p]
		if rec.expire < currentTime {
			newCacheSize--
			delete(cache.cachedRecs, p)
		}
	}

	cookie := cache.cookie

	// Discover new records if we don't have enough
	if newCacheSize < limit {
		// TODO: Should we return error even if we have valid cached results?
		var regs []Registration
		var newCookie []byte
		if regs, newCookie, err = c.rp.Discover(ctx, ns, limit, cookie); err == nil {
			if cache.cachedRecs == nil {
				cache.cachedRecs = make(map[peer.ID]*record)
			}
			for _, reg := range regs {
				rec := &record{peer: reg.Peer, expire: int64(reg.Ttl) + currentTime}
				cache.cachedRecs[rec.peer.ID] = rec
			}
			cache.cookie = newCookie
		}
	}

	// Randomize and fill channel with available records
	count := len(cache.cachedRecs)
	if limit < count {
		count = limit
	}

	chPeer := make(chan peer.AddrInfo, count)

	c.rngMux.Lock()
	perm := c.rng.Perm(len(cache.cachedRecs))[0:count]
	c.rngMux.Unlock()

	permSet := make(map[int]int)
	for i, v := range perm {
		permSet[v] = i
	}

	sendLst := make([]*peer.AddrInfo, count)
	iter := 0
	for k := range cache.cachedRecs {
		if sendIndex, ok := permSet[iter]; ok {
			sendLst[sendIndex] = &cache.cachedRecs[k].peer
		}
		iter++
	}

	for _, send := range sendLst {
		chPeer <- *send
	}

	close(chPeer)
	return chPeer, err
}
