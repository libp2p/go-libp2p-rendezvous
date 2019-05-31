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

type rendezvousDiscoveryClient struct {
	rp        RendezvousPoint
	peerCache sync.Map //is a map[string]discoveredPeerCache
	rng       *rand.Rand
	rngMux    sync.Mutex
}

type discoveredPeerCache struct {
	cachedRegs map[peer.ID]*Registration
	cookie     []byte
	mux        sync.Mutex
}

func NewRendezvousDiscoveryClient(host host.Host, rendezvousPeer peer.ID) discovery.Discovery {
	rp := NewRendezvousPoint(host, rendezvousPeer)
	return &rendezvousDiscoveryClient{rp, sync.Map{}, rand.New(rand.NewSource(rand.Int63())), sync.Mutex{}}
}

func (c *rendezvousDiscoveryClient) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
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

func (c *rendezvousDiscoveryClient) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
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

	// Remove all expired entries from cache
	currentTime := int(time.Now().Unix())
	newCacheSize := len(cache.cachedRegs)

	for p := range cache.cachedRegs {
		reg := cache.cachedRegs[p]
		if reg.Ttl < currentTime {
			newCacheSize--
			delete(cache.cachedRegs, p)
		}
	}

	cookie := cache.cookie
	cache.mux.Unlock()

	// Discover new records if we don't have enough
	var discoveryErr error
	if newCacheSize < limit {
		if discoveryRecords, newCookie, err := c.rp.Discover(ctx, ns, limit, cookie); err == nil {
			cache.mux.Lock()
			if cache.cachedRegs == nil {
				cache.cachedRegs = make(map[peer.ID]*Registration)
			}
			for i := range discoveryRecords {
				rec := &discoveryRecords[i]
				rec.Ttl += currentTime
				cache.cachedRegs[rec.Peer.ID] = rec
			}
			cache.cookie = newCookie
			cache.mux.Unlock()
		} else {
			// TODO: Should we return error even if we have valid cached results?
			discoveryErr = err
		}
	}

	// Randomize and fill channel with available records
	cache.mux.Lock()
	sendQuantity := len(cache.cachedRegs)
	if limit < sendQuantity {
		sendQuantity = limit
	}

	chPeer := make(chan peer.AddrInfo, sendQuantity)

	c.rngMux.Lock()
	perm := c.rng.Perm(len(cache.cachedRegs))[0:sendQuantity]
	c.rngMux.Unlock()

	permSet := make(map[int]int)
	for i, v := range perm {
		permSet[v] = i
	}

	sendLst := make([]*peer.AddrInfo, sendQuantity)
	iter := 0
	for k := range cache.cachedRegs {
		if sendIndex, ok := permSet[iter]; ok {
			sendLst[sendIndex] = &cache.cachedRegs[k].Peer
		}
		iter++
	}

	for _, send := range sendLst {
		chPeer <- *send
	}

	cache.mux.Unlock()
	close(chPeer)
	return chPeer, discoveryErr
}
