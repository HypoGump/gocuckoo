package cuckoo

import (
	"fmt"
)

const (
	defaultKickNum    = 500
	defaultBucketSize = 8
)

// Filter64 in this package is like bloom filter with reversable feature
type Filter64 struct {
	// Number of items stored in cuckoo filter
	itemsNum uint64
	capacity uint64
	// Maximum kicks before returning failure
	maxKickNum uint
	// Hashtable where items stored
	table *hashtable64
	hfunc filterHashFunc
}

// NewDefaultFilter create a cuckoo filter with built-in hash functions
func NewDefaultFilter(capacity, bsize, maxKickNum uint) *Filter64 {
	if bsize == 0 {
		bsize = defaultBucketSize
	}

	if maxKickNum == 0 {
		maxKickNum = defaultKickNum
	}

	return &Filter64{
		capacity:   uint64(capacity) * uint64(bsize),
		maxKickNum: maxKickNum,
		table:      newHashtable64(capacity, bsize),
		hfunc:      newDefaultFilterHashFunc(),
	}
}

// Insert will insert a item into cuckoo filter's hashtable if insertion fails, return error
func (cf64 *Filter64) Insert(item []byte) error {
	i1, i2, fp := cf64.hfunc.GetBucketCandidateAndFingerprint(item)

	_, ret := cf64.table.insert(i1, fp, false)
	if ret == retInsertSucceed {
		return nil
	}

	for cnt := uint(0); cnt < cf64.maxKickNum; cnt++ {
		victim, ret := cf64.table.insert(i2, fp, true)
		if ret == retInsertSucceed {
			return nil
		} else if ret == retInsertEvictItem {
			i2 = i2 ^ cf64.hfunc.HashFingerprint(victim)
			fp = victim
		}
	}
	return fmt.Errorf("insertion failed(reach max kick num)")
}

// Delete will delete a existed item from the table if item is nonvalid, it will do nothing
func (cf64 *Filter64) Delete(item []byte) error {
	i1, i2, fp := cf64.hfunc.GetBucketCandidateAndFingerprint(item)
	ok := cf64.table.delete(i1, fp)
	if ok {
		return nil
	}
	cf64.table.delete(i2, fp)
	if ok {
		return nil
	}
	return fmt.Errorf("item is not existed")
}

// Lookup will search item in table, if it exists, return true, otherwise return false
func (cf64 *Filter64) Lookup(item []byte) bool {
	i1, i2, fp := cf64.hfunc.GetBucketCandidateAndFingerprint(item)
	return cf64.table.lookup(i1, fp) || cf64.table.lookup(i2, fp)
}
