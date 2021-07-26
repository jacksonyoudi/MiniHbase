package org.youdi.minihbase;

import org.youdi.minihbase.DiskStore.DefaultCompactor;
import org.youdi.minihbase.DiskStore.DefaultFlusher;
import org.youdi.minihbase.DiskStore.MultiIter;
import org.youdi.minihbase.KeyValue.Op;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MStore implements org.youdi.minihbase.MiniBase {

  private ExecutorService pool;
  private org.youdi.minihbase.MemStore memStore;
  private org.youdi.minihbase.DiskStore diskStore;
  private Compactor compactor;
  private AtomicLong sequenceId;

  private Config conf;

  public org.youdi.minihbase.MiniBase open() throws IOException {
    assert conf != null;

    // initialize the thread pool;
    this.pool = Executors.newFixedThreadPool(conf.getMaxThreadPoolSize());

    // initialize the disk store.
    this.diskStore = new org.youdi.minihbase.DiskStore(conf.getDataDir(), conf.getMaxDiskFiles());
    this.diskStore.open();
    // TODO initialize the max sequence id here.
    this.sequenceId = new AtomicLong(0);

    // initialize the memstore.
    this.memStore = new org.youdi.minihbase.MemStore(conf, new DefaultFlusher(diskStore), pool);

    this.compactor = new DefaultCompactor(diskStore);
    this.compactor.start();
    return this;
  }

  private MStore(Config conf) {
    this.conf = conf;
  }

  public static MStore create(Config conf) {
    return new MStore(conf);
  }

  public static MStore create() {
    return create(Config.getDefault());
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    this.memStore.add(org.youdi.minihbase.KeyValue.createPut(key, value, sequenceId.incrementAndGet()));
  }

  @Override
  public org.youdi.minihbase.KeyValue get(byte[] key) throws IOException {
    org.youdi.minihbase.KeyValue result = null;
    Iter<org.youdi.minihbase.KeyValue> it = scan(key, Bytes.EMPTY_BYTES);
    if (it.hasNext()) {
      org.youdi.minihbase.KeyValue kv = it.next();
      if (Bytes.compare(kv.getKey(), key) == 0) {
        result = kv;
      }
    }
    return result;
  }

  @Override
  public void delete(byte[] key) throws IOException {
    this.memStore.add(org.youdi.minihbase.KeyValue.createDelete(key, sequenceId.incrementAndGet()));
  }

  @Override
  public Iter<org.youdi.minihbase.KeyValue> scan(byte[] start, byte[] stop) throws IOException {
    List<SeekIter<org.youdi.minihbase.KeyValue>> iterList = new ArrayList<>();
    iterList.add(memStore.createIterator());
    iterList.add(diskStore.createIterator());
    MultiIter it = new MultiIter(iterList);

    // with start being EMPTY_BYTES means minus infinity, will skip to seek.
    if (Bytes.compare(start, Bytes.EMPTY_BYTES) != 0) {
      it.seekTo(org.youdi.minihbase.KeyValue.createDelete(start, sequenceId.get()));
    }

    org.youdi.minihbase.KeyValue stopKV = null;
    if (Bytes.compare(stop, Bytes.EMPTY_BYTES) != 0) {
      // the smallest kv in all KeyValue with the same key.
      stopKV = org.youdi.minihbase.KeyValue.createDelete(stop, Long.MAX_VALUE);
    }
    return new ScanIter(stopKV, it);
  }

  static class ScanIter implements Iter<org.youdi.minihbase.KeyValue> {

    private org.youdi.minihbase.KeyValue stopKV;
    private Iter<org.youdi.minihbase.KeyValue> storeIt;
    // Last KV is the last key value which has the largest sequence id in key values with the
    // same key, but diff sequence id or op.
    private org.youdi.minihbase.KeyValue lastKV = null;
    private org.youdi.minihbase.KeyValue pendingKV = null;

    public ScanIter(org.youdi.minihbase.KeyValue stopKV, SeekIter<org.youdi.minihbase.KeyValue> it) {
      this.stopKV = stopKV;
      this.storeIt = it;
    }

    @Override
    public boolean hasNext() throws IOException {
      if (pendingKV == null) {
        switchToNewKey();
      }
      return pendingKV != null;
    }

    private boolean shouldStop(org.youdi.minihbase.KeyValue kv) {
      return stopKV != null && Bytes.compare(stopKV.getKey(), kv.getKey()) <= 0;
    }

    private void switchToNewKey() throws IOException {
      if (lastKV != null && shouldStop(lastKV)) {
        return;
      }
      org.youdi.minihbase.KeyValue curKV;
      while (storeIt.hasNext()) {
        curKV = storeIt.next();
        if (shouldStop(curKV)) {
          return;
        }
        if (curKV.getOp() == Op.Put) {
          if (lastKV == null) {
            lastKV = pendingKV = curKV;
            return;
          }
          int ret = Bytes.compare(lastKV.getKey(), curKV.getKey());
          if (ret < 0) {
            lastKV = pendingKV = curKV;
            return;
          } else if (ret > 0) {
            String msg = "KV mis-encoded, curKV < lastKV, curKV:" + Bytes.toHex(curKV.getKey()) +
                         ", lastKV:" + Bytes.toHex(lastKV.getKey());
            throw new IOException(msg);
          }
          // Same key with lastKV, should continue to fetch the next key value.
        } else if (curKV.getOp() == Op.Delete) {
          if (lastKV == null || Bytes.compare(lastKV.getKey(), curKV.getKey()) != 0) {
            lastKV = curKV;
          }
        } else {
          throw new IOException("Unknown op code: " + curKV.getOp());
        }
      }
    }

    @Override
    public org.youdi.minihbase.KeyValue next() throws IOException {
      if (pendingKV == null) {
        switchToNewKey();
      }
      lastKV = pendingKV;
      pendingKV = null;
      return lastKV;
    }
  }

  @Override
  public void close() throws IOException {
    memStore.close();
    diskStore.close();
    compactor.interrupt();
  }

  interface SeekIter<KeyValue> extends Iter<KeyValue> {

    /**
     * Seek to the smallest key value which is greater than or equals to the given key value.
     *
     * @param kv
     */
    void seekTo(KeyValue kv) throws IOException;
  }
}
