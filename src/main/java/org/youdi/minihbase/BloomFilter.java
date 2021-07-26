package org.youdi.minihbase;

public class BloomFilter {
    private int k; // 每个key hash次数
    private int bitsPerKey; // 每个key占用的二进制的bit数
    private int bitLen;
    private byte[] result;


    public BloomFilter(int k, int bitsPerKey) {
        this.k = k;
        this.bitsPerKey = bitsPerKey;
    }

    public byte[] generate(byte[][] keys) {
        assert keys != null;
        bitLen = keys.length * bitsPerKey;
        bitLen = ((bitLen + 7) / 8) << 3; // 进一
        bitLen = bitLen < 64 ? 64 : bitLen;
        result = new byte[bitLen >> 3];
        for (int i = 0; i < keys.length; i++) {
            assert keys[i] != null;
            int h = Bytes.hash(keys[i]);
            for (int t = 0; t < k; t++) {
                int idx = (h % bitLen + bitLen) % bitLen;
                result[idx / 8] |= (1 << (idx % 8));
                int delta = (h >> 17) | (h << 15);
                h += delta;
            }
        }
        return result;
    }

    public boolean contains(byte[] key) {
        assert result != null;
        int h = Bytes.hash(key);
        for (int t = 0; t < k; t++) {
            int idx = (h % bitLen + bitLen) % bitLen;
            if ((result[idx / 8] & (1 << (idx % 8))) == 0) {
                return false;
            }
            int delta = (h >> 17) | (h << 15);
            h += delta;
        }
        return true;
    }
}
