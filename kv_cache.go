package mode

import (
	"errors"
	"time"
)

type (
	keyValueSync struct {
		Action   string      `json:"action"`
		Revision int         `json:"rev"`
		Key      string      `json:"key"`
		Value    interface{} `json:"value"`
		NumItems int         `json:"numItems"`
		Items    []*KeyValue `json:"items"`
	}

	keyValueCacheItem struct {
		revision         int
		value            interface{}
		modificationTime time.Time
		deleted          bool
	}

	keyValueCache struct {
		initialized bool
		items       map[string]*keyValueCacheItem
		syncQueue   chan *keyValueSync
		access      chan func()
	}
)

const (
	kvSyncActionReload = "reload"
	kvSyncActionSet    = "set"
	kvSyncActionDelete = "delete"
)

var (
	ErrCacheNotReady  = errors.New("cache not ready")
	ErrKeyNotFound    = errors.New("key not found")
	ErrObsoleteUpdate = errors.New("obsolete update")
)

func newKeyValueCache() *keyValueCache {
	return &keyValueCache{
		syncQueue: make(chan *keyValueSync, keyValueSyncQueueLength),
		access:    make(chan func()),
	}
}

func (cache *keyValueCache) run() {
	logInfo("[keyValueCache] starting processing loop")
	defer logInfo("[keyValueCache] exiting processing loop")

	for {
		select {
		case kvSync := <-cache.syncQueue:
			if kvSync == nil {
				return
			}

			cache.handleSync(kvSync)

		case f := <-cache.access:
			f()
		}
	}
}

func (cache *keyValueCache) terminate() {
	if cache.syncQueue != nil {
		close(cache.syncQueue)
		cache.syncQueue = nil
	}
}

func (cache *keyValueCache) syncReload(kvSync *keyValueSync) error {
	cacheItems := make(map[string]*keyValueCacheItem)

	for _, item := range kvSync.Items {
		cacheItems[item.Key] = &keyValueCacheItem{
			revision:         kvSync.Revision,
			value:            item.Value,
			modificationTime: time.Now(),
		}
	}

	logInfo("[keyValueCache] downloaded %d items", len(cacheItems))
	cache.items = cacheItems
	cache.initialized = true

	return nil
}

func (cache *keyValueCache) syncSet(kvSync *keyValueSync) error {
	if !cache.initialized {
		return ErrCacheNotReady
	}

	if cacheItem, ok := cache.items[kvSync.Key]; ok {
		if cacheItem.revision >= kvSync.Revision {
			logInfo("[keyValueCache] ignored obsolete update (rev %d) to key '%s' (rev %d)", kvSync.Revision, kvSync.Key, cacheItem.revision)
			return ErrObsoleteUpdate
		}

		cacheItem.revision = kvSync.Revision
		cacheItem.value = kvSync.Value
		cacheItem.modificationTime = time.Now()
		cacheItem.deleted = false // just in case item was previously deleted
		logInfo("[keyValueCache] updated value of key '%s' (new rev %d)", kvSync.Key, kvSync.Revision)
	} else {
		cache.items[kvSync.Key] = &keyValueCacheItem{
			revision:         kvSync.Revision,
			value:            kvSync.Value,
			modificationTime: time.Now(),
		}
		logInfo("[keyValueCache] saved new key '%s' (new rev %d)", kvSync.Key, kvSync.Revision)
	}

	return nil
}

func (cache *keyValueCache) syncDelete(kvSync *keyValueSync) error {
	if !cache.initialized {
		return ErrCacheNotReady
	}

	if cacheItem, ok := cache.items[kvSync.Key]; ok {
		if cacheItem.revision >= kvSync.Revision {
			logInfo("[keyValueCache] ignored obsolete update (rev %d) to key '%s' (rev %d)", kvSync.Revision, kvSync.Key, cacheItem.revision)
			return ErrObsoleteUpdate
		}

		// Mark item as deleted.
		cacheItem.revision = kvSync.Revision
		cacheItem.modificationTime = time.Now()
		cacheItem.deleted = true
		logInfo("[keyValueCache] deleted key '%s' (new rev %d)", kvSync.Key, kvSync.Revision)
	} else {
		// This can happen if SET and DELETE transactions are out of order.
		// Record the delete anyway.
		cache.items[kvSync.Key] = &keyValueCacheItem{
			revision:         kvSync.Revision,
			modificationTime: time.Now(),
			deleted:          true,
		}
	}

	return nil
}

func (cache *keyValueCache) handleSync(kvSync *keyValueSync) {
	switch kvSync.Action {
	case kvSyncActionReload:
		if err := cache.syncReload(kvSync); err == nil {
			// callback
		}

	case kvSyncActionSet:
		if err := cache.syncSet(kvSync); err == nil {
			// callback
		}

	case kvSyncActionDelete:
		if err := cache.syncDelete(kvSync); err == nil {
			// callback
		}

	default:
		logError("[keyValueCache] received sync message with unknown action '%s'", kvSync.Action)
	}
}

func (cache *keyValueCache) getKeyValue(key string) (*KeyValue, error) {
	resErr := make(chan error)
	resKV := make(chan *KeyValue)

	cache.access <- func() {
		if !cache.initialized {
			resErr <- ErrCacheNotReady
			return
		}

		if cacheItem, ok := cache.items[key]; ok && !cacheItem.deleted {
			resKV <- &KeyValue{
				Key:              key,
				Value:            cacheItem.value,
				ModificationTime: cacheItem.modificationTime,
			}
			return
		}

		resErr <- ErrKeyNotFound
	}

	select {
	case err := <-resErr:
		return nil, err
	case kv := <-resKV:
		return kv, nil
	}
}
