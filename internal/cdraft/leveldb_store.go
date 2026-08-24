package cdraft

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// syncWrite forces every batch to be flushed (fsync) to stable storage before
// Write returns. Consensus durability requires this: a node that acknowledges a
// replicated/committed entry must not lose it on a crash/power loss, otherwise a
// restarted node could silently roll back state the cluster believes is durable.
var syncWrite = &opt.WriteOptions{Sync: true}
var asyncWrite = &opt.WriteOptions{Sync: false}

// leveldbEngine adapts goleveldb to the KVEngine interface.
type leveldbEngine struct {
	db           *leveldb.DB
	writeOptions *opt.WriteOptions
}

func (e *leveldbEngine) Get(key []byte) ([]byte, bool, error) {
	value, err := e.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (e *leveldbEngine) Write(ops []KVOp) error {
	batch := new(leveldb.Batch)
	for _, op := range ops {
		if op.Delete {
			batch.Delete(op.Key)
		} else {
			batch.Put(op.Key, op.Value)
		}
	}
	return e.db.Write(batch, e.writeOptions)
}

func (e *leveldbEngine) Scan(prefix []byte, fn func(key, value []byte) error) error {
	iter := e.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()
	for iter.Next() {
		if err := fn(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (e *leveldbEngine) Close() error {
	return e.db.Close()
}

// NewLevelDBStore opens (creating if needed) a LevelDB database at dir and wraps
// it as a consensus-facing Store. It is a thin convenience over
// NewKVStore(<leveldb engine>); swap in another engine for RocksDB/BoltDB/etc.
func NewLevelDBStore(dir string) (*KVStore, error) {
	return NewLevelDBStoreWithSync(dir, true)
}

// NewLevelDBStoreWithSync opens a LevelDB-backed Store and lets benchmarks opt
// out of per-batch fsync. Production/default callers should use NewLevelDBStore.
func NewLevelDBStoreWithSync(dir string, durableSync bool) (*KVStore, error) {
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return nil, fmt.Errorf("open leveldb store: %w", err)
	}
	writeOptions := syncWrite
	if !durableSync {
		writeOptions = asyncWrite
	}
	return NewKVStore(&leveldbEngine{db: db, writeOptions: writeOptions}), nil
}
