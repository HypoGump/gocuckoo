package cuckoo

import "math/rand"

const (
	retInsertSucceed = iota
	retInsertEvictItem
	retInsertFailed
)

// fingerprint size is 8 bits
type bucket []uint8
type flagBucket []bool

type hashtable64 struct {
	buckets  []bucket
	flags    []flagBucket
	bsize    uint
	capacity uint
}

func newHashtable64(capacity, bsize uint) *hashtable64 {
	ht64 := &hashtable64{
		buckets: make([]bucket, capacity),
		flags:   make([]flagBucket, capacity),
		bsize:   bsize,
	}

	for i := uint(0); i < capacity; i++ {
		ht64.buckets[i] = make([]uint8, bsize)
		ht64.flags[i] = make([]bool, bsize)
	}
	return ht64
}

// insert will insert a given fingerprint into the hashtable, and returns the item evicted
func (ht64 *hashtable64) insert(key uint64, fp uint8, force bool) (uint8, int) {
	index := key % uint64(ht64.capacity)
	f := ht64.flags[index]
	b := ht64.buckets[index]

	// if there is a empty entry, insert and return
	for i := uint(0); i < ht64.bsize; i++ {
		if !f[i] {
			b[i] = fp
			f[i] = true
			return 0, retInsertSucceed
		}
	}

	// if bucket is full and force flag is set, we will evict a item and insert the new one
	if force {
		victimIdx := rand.Intn(int(ht64.bsize))
		victim := b[victimIdx]
		b[victimIdx] = fp
		return victim, retInsertEvictItem
	}
	return 0, retInsertFailed
}

func (ht64 *hashtable64) delete(key uint64, fp uint8) bool {
	index := key % uint64(ht64.capacity)
	f := ht64.flags[index]
	b := ht64.buckets[index]
	for i := uint(0); i < ht64.bsize; i++ {
		if f[i] && b[i] == fp {
			f[i] = false
			return true
		}
	}
	return false
}

func (ht64 *hashtable64) lookup(key uint64, fp uint8) bool {
	index := key % uint64(ht64.capacity)
	f := ht64.flags[index]
	b := ht64.buckets[index]
	for i := uint(0); i < ht64.bsize; i++ {
		if f[i] && b[i] == fp {
			return true
		}
	}
	return false
}
