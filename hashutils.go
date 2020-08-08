package cuckoo

import (
	"encoding/binary"
	"hash/maphash"
)

// NewHashFunc will return a rand hash function.
// A zero Hash chooses a random seed for itself during the first call to Reset, Write, Seed, Sum64, or Seed method.
// So if you want identical hash functions, use SetSeed to set common seed.
func NewHashFunc() *maphash.Hash {
	return &maphash.Hash{}
}

// FilterHashFunc represents all methods which hash functions should implement
type filterHashFunc interface {
	HashFingerprint(uint8) uint64
	GetFingerprint([]byte) uint8
	GetBucketCandidateAndFingerprint([]byte) (uint64, uint64, uint8)
}

// default hash function for cuckoo filter
type defaultFilterHashFunc struct {
	h1 *maphash.Hash
	h2 *maphash.Hash
}

func newDefaultFilterHashFunc() *defaultFilterHashFunc {
	return &defaultFilterHashFunc{
		h1: NewHashFunc(),
		h2: NewHashFunc(),
	}
}

func (dhf *defaultFilterHashFunc) HashFingerprint(fp uint8) uint64 {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, uint64(fp))
	dhf.h2.Write(buf)
	hval := dhf.h2.Sum64()
	dhf.h2.Reset()
	return hval
}

func (dhf *defaultFilterHashFunc) GetFingerprint(data []byte) uint8 {
	dhf.h1.Write(data)
	fp64 := dhf.h1.Sum64()
	dhf.h1.Reset()
	return uint8(fp64 & 0xff)
}

func (dhf *defaultFilterHashFunc) GetBucketCandidateAndFingerprint(data []byte) (uint64, uint64, uint8) {
	fp := dhf.GetFingerprint(data)
	// i1 = hash(x)
	dhf.h2.Write(data)
	i1 := dhf.h2.Sum64()

	dhf.h2.Reset()

	// i2 = i1 ^ hash(fp)
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, uint64(fp))
	dhf.h2.Write(buf)
	i2 := i1 ^ dhf.h2.Sum64()

	dhf.h2.Reset()
	return i1, i2, fp
}
