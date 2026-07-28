from bitarray import bitarray
import mmh3
from pyroaring import BitMap

class BloomFilter:

    def __init__(self, size=1000, num_hashes=3):
        self.bits = bitarray(size)
        self.bits.setall(False)
        self.size = size
        self.num_hashes = num_hashes
    
    def add(self, item):
        for seed in range(self.num_hashes):
            #hash value of item
            #combine with seed
            # insert into filter
            idx = mmh3.hash(item, seed) % self.size
            self.bits[idx] = True

    def contains(self, item):
        for seed in range(self.num_hashes):
            idx = mmh3.hash(item, seed) % self.size
            if not self.bits[idx]:
                return False
        
        return True

def main_bf_bitarray():
    bf = BloomFilter()
    bf.add("actor_123")
    bf.add("actor_456")
    print(bf.contains("actor_123"))
    print(bf.contains("actor_456"))
    print(bf.contains("actor_789"))


class RoaringBitmap:

    def __init__(self):
        self.bitmap = BitMap()
    
    def add(self, item):
        hash_val = mmh3.hash(item, signed=False)
        self.bitmap.add(hash_val)

    def contains(self, item):
        hash_val = mmh3.hash(item, signed=False)
        return hash_val in self.bitmap
    
    def remove(self, item):
        hash_val = mmh3.hash(item, signed=False)
        self.bitmap.discard(item)

def main_bf_roaring():
    rb = RoaringBitmap()
    rb.add("actor_123")
    rb.add("actor_456")
    print(rb.contains("actor_123"))
    print(rb.contains("actor_456"))
    print(rb.contains("actor_789"))

if __name__ == "__main__":

    main_bf_bitarray()
    main_bf_roaring()