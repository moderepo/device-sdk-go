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
		dc          *DeviceContext
		initialized bool
		items       map[string]*keyValueCacheItem
		syncQueue   chan *keyValueSync // for pulling changes from cloud
		pushQueue   chan *keyValueSync // for pushing changes to cloud
		access      chan func()
	}
)

const (
	kvSyncActionReload = "reload"
	kvSyncActionSet    = "set"
	kvSyncActionDelete = "delete"
)

var (
	ErrorKeyValuesNotReady = errors.New("key-values not ready")
	ErrorKeyNotFound       = errors.New("key not found")
)

func newKeyValueCache(dc *DeviceContext) *keyValueCache {
	return &keyValueCache{
		dc:        dc,
		syncQueue: make(chan *keyValueSync, keyValueSyncQueueLength),
		pushQueue: make(chan *keyValueSync, keyValuePushQueueLength),
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

	if cache.pushQueue != nil {
		close(cache.pushQueue)
		cache.pushQueue = nil
	}
}

func (cache *keyValueCache) syncReload(kvSync *keyValueSync) {
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

	if keyValuesReadyCallback != nil {
		go keyValuesReadyCallback(cache.dc)
	}
}

func (cache *keyValueCache) syncSet(kvSync *keyValueSync) {
	if !cache.initialized {
		logInfo("[keyValueCache] ignored update to key '%s' because cache is not yet initialized", kvSync.Key)
		return
	}

	cacheItem, ok := cache.items[kvSync.Key]
	if ok {
		if cacheItem.revision >= kvSync.Revision {
			logInfo("[keyValueCache] ignored obsolete update (rev %d) to key '%s' (rev %d)", kvSync.Revision, kvSync.Key, cacheItem.revision)
			return
		}
	} else {
		cacheItem = &keyValueCacheItem{}
		cache.items[kvSync.Key] = cacheItem
	}

	cacheItem.revision = kvSync.Revision
	cacheItem.value = kvSync.Value
	cacheItem.modificationTime = time.Now()
	cacheItem.deleted = false // just in case item was previously deleted
	logInfo("[keyValueCache] saved key '%s' (new rev %d)", kvSync.Key, kvSync.Revision)

	if keyValueStoredCallback != nil {
		go keyValueStoredCallback(cache.dc, &KeyValue{Key: kvSync.Key, Value: kvSync.Value, ModificationTime: cacheItem.modificationTime})
	}
}

func (cache *keyValueCache) syncDelete(kvSync *keyValueSync) {
	if !cache.initialized {
		logInfo("[keyValueCache] ignored delete to key '%s' because cache is not yet initialized", kvSync.Key)
		return
	}

	cacheItem, ok := cache.items[kvSync.Key]
	if ok {
		if cacheItem.revision >= kvSync.Revision {
			logInfo("[keyValueCache] ignored obsolete update (rev %d) to key '%s' (rev %d)", kvSync.Revision, kvSync.Key, cacheItem.revision)
			return
		}
	} else {
		// This can happen if SET and DELETE transactions are out of order.
		// Record the delete anyway.
		cacheItem = &keyValueCacheItem{}
		cache.items[kvSync.Key] = cacheItem
	}

	// Mark item as deleted.
	cacheItem.revision = kvSync.Revision
	cacheItem.modificationTime = time.Now()
	cacheItem.deleted = true
	logInfo("[keyValueCache] deleted key '%s' (new rev %d)", kvSync.Key, kvSync.Revision)

	if keyValueDeletedCallback != nil {
		go keyValueDeletedCallback(cache.dc, kvSync.Key)
	}
}

func (cache *keyValueCache) handleSync(kvSync *keyValueSync) {
	switch kvSync.Action {
	case kvSyncActionReload:
		cache.syncReload(kvSync)
	case kvSyncActionSet:
		cache.syncSet(kvSync)
	case kvSyncActionDelete:
		cache.syncDelete(kvSync)
	default:
		logError("[keyValueCache] received sync message with unknown action '%s'", kvSync.Action)
	}
}

func (cache *keyValueCache) getKeyValue(key string) (*KeyValue, error) {
	resErr := make(chan error)
	resKV := make(chan *KeyValue)

	cache.access <- func() {
		if !cache.initialized {
			resErr <- ErrorKeyValuesNotReady
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

		resErr <- ErrorKeyNotFound
	}

	select {
	case err := <-resErr:
		return nil, err
	case kv := <-resKV:
		return kv, nil
	}
}

func (cache *keyValueCache) getAllKeyValues() ([]*KeyValue, error) {
	resErr := make(chan error)
	resKV := make(chan *KeyValue)

	cache.access <- func() {
		if !cache.initialized {
			resErr <- ErrorKeyValuesNotReady
			return
		}

		for key, cacheItem := range cache.items {
			if cacheItem.deleted {
				continue
			}

			resKV <- &KeyValue{
				Key:              key,
				Value:            cacheItem.value,
				ModificationTime: cacheItem.modificationTime,
			}
		}

		close(resKV)
	}

	res := make([]*KeyValue, 0, 10)

	for {
		select {
		case err := <-resErr:
			return nil, err
		case kv := <-resKV:
			if kv == nil {
				return res, nil
			}

			res = append(res, kv)
		}
	}
}

func (cache *keyValueCache) setKeyValue(key string, value interface{}) error {
	resErr := make(chan error)

	cache.access <- func() {
		if !cache.initialized {
			resErr <- ErrorKeyValuesNotReady
			return
		}

		cacheItem, ok := cache.items[key]
		if !ok {
			cacheItem = &keyValueCacheItem{}
			cache.items[key] = cacheItem
		}

		// We intentionally avoid incrementing the revision of the item.
		cacheItem.value = value
		cacheItem.modificationTime = time.Now()
		cacheItem.deleted = false // just in case item was previously deleted

		cache.pushQueue <- &keyValueSync{
			Action: kvSyncActionSet,
			Key:    key,
			Value:  value,
		}

		resErr <- nil
	}

	return <-resErr
}

func (cache *keyValueCache) deleteKeyValue(key string) error {
	resErr := make(chan error)

	cache.access <- func() {
		if !cache.initialized {
			resErr <- ErrorKeyValuesNotReady
			return
		}

		if cacheItem, ok := cache.items[key]; ok {
			// Mark item as deleted.
			// We intentionally avoid incrementing the revision of the item.
			cacheItem.deleted = true
			cacheItem.modificationTime = time.Now()

			cache.pushQueue <- &keyValueSync{
				Action: kvSyncActionDelete,
				Key:    key,
			}

			resErr <- nil
		} else {
			resErr <- ErrorKeyNotFound
		}
	}

	return <-resErr
}
