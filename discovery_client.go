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
	cachedRegs []Registration
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

	// Default is minimum duration
	if ttl == 0 {
		ttlSeconds = 120
	} else {
		ttlSeconds = int(math.Round(ttl.Seconds()))
	}

	if err := c.rp.Register(ctx, ns, ttlSeconds); err != nil {
		return 0, err
	}

	actualTTL := time.Duration(ttlSeconds) * time.Second
	return actualTTL, nil
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
	cachedRegs := cache.cachedRegs

	// Remove all expired entries from cache
	currentTime := int(time.Now().Unix())
	newCacheSize := len(cachedRegs)

	for i := 0; i < newCacheSize; i++ {
		reg := cachedRegs[i]
		if reg.Ttl < currentTime {
			newCacheSize--
			if i != newCacheSize {
				cachedRegs[i] = cachedRegs[newCacheSize]
				i--
			}
		}
	}
	cache.cachedRegs = cachedRegs[:newCacheSize]
	cache.mux.Unlock()

	// Discover new records if we don't have enough
	var discoveryErr error
	if newCacheSize < limit {
		if discoveryRecords, _, err := c.rp.Discover(ctx, ns, limit, nil); err == nil {
			for i := range discoveryRecords {
				discoveryRecords[i].Ttl += currentTime
			}
			cache.mux.Lock()
			cache.cachedRegs = discoveryRecords
			cache.mux.Unlock()
		} else {
			// TODO: Should we return error even if we have valid cached results?
			discoveryErr = err
		}
	}

	// Randomize and fill channel with available records
	cache.mux.Lock()
	cachedRegs = cache.cachedRegs
	sendQuantity := len(cachedRegs)
	if limit < sendQuantity {
		sendQuantity = limit
	}

	chPeer := make(chan peer.AddrInfo, sendQuantity)

	c.rngMux.Lock()
	perm := c.rng.Perm(len(cachedRegs))[0:sendQuantity]
	c.rngMux.Unlock()

	for _, i := range perm {
		chPeer <- cachedRegs[i].Peer
	}

	cache.mux.Unlock()
	close(chPeer)
	return chPeer, discoveryErr
}
