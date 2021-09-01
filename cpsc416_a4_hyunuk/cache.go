package distpow

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"
)

type CacheAdd struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheRemove struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheHit struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type CacheMiss struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CacheKey struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CacheEntry struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type Cache struct {
	mu 		 sync.Mutex
	cacheMap map[string]*CacheEntry
}

func NewCache() *Cache {
	return &Cache{
		cacheMap: map[string]*CacheEntry{},
	}
}

func (c *Cache) Add(entry CacheEntry) (*CacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	existed := c.cacheMap[generateCacheKey(entry.Nonce)]
	if existed == nil {
		c.cacheMap[generateCacheKey(entry.Nonce)] = &entry
		return nil, true
	}
	if existed.NumTrailingZeros == entry.NumTrailingZeros {
		if bytes.Compare(existed.Secret, entry.Secret) < 0 {
			c.cacheMap[generateCacheKey(entry.Nonce)] = &entry
			return existed, true
		}
	}
	//if existed == nil || existed.NumTrailingZeros < entry.NumTrailingZeros || bytes.Compare(existed.Secret, entry.Secret) < 0 {
	//	c.cacheMap[generateCacheKey(entry.Nonce)] = &entry
	//	return existed
	//}
	//return nil
	return nil, false
}

func (c *Cache) Get(key CacheKey) []uint8 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.cacheMap[generateCacheKey(key.Nonce)]; e != nil && e.NumTrailingZeros >= key.NumTrailingZeros {
		return e.Secret
	}
	return nil
}

func isNewGreater(oldVal []uint8, newVal []uint8) bool {
	if len(oldVal) < len(newVal) {
		return true
	} else if len(oldVal) > len(newVal) {
		return false
	}
	for i := range oldVal {
		if newVal[i] > oldVal[i] {
			return true
		} else if oldVal[i] > newVal[i] {
			return false
		}
	}
	return false
}

func generateCacheKey(nonce []uint8) string {
	return fmt.Sprintf("%s", hex.EncodeToString(nonce))
}
