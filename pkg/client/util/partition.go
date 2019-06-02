package util

import "hash/fnv"

// GetPartitionIndex returns the index of the partition for the given key
func GetPartitionIndex(key string, partitions int) (int, error) {
	h := fnv.New32a()
	if _, err := h.Write([]byte(key)); err != nil {
		return 0, err
	}
	return int(h.Sum32() % uint32(partitions)), nil
}
