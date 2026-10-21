(ns mnemonic.rocks-sync
  "WAL sync for group commit. Lives outside mnemonic.rocks (owned by another piece); it
  follows the same store-guard protocol: the call is counted in the Store's in-flight
  guard so `close-store` waits for it, and a closed store throws connection-released
  instead of reaching native code."
  (:require [mnemonic.rocks :as rocks])
  (:import (java.util.concurrent.atomic AtomicBoolean AtomicLongArray)
           (mnemonic.rocks Guard Store)
           (org.rocksdb RocksDB RocksDBException)))

(set! *warn-on-reflection* true)

(defn- guard-slot ^long []
  ;; same striping as mnemonic.rocks/guard-slot (16 stripes, 8 longs apart)
  (bit-shift-left (bit-and (.threadId (Thread/currentThread)) 15) 3))

(defn sync-wal!
  "Flushes the write-ahead log to the OS and fsyncs it: every write already accepted by
  the store (including `sync? false` writes) is on stable storage when this returns."
  [^Store store]
  (let [^AtomicLongArray c (.-counts ^Guard (.-guard store))
        closed ^AtomicBoolean (.-closed store)
        i (guard-slot)]
    (when (.get closed) (throw (rocks/connection-released-ex)))
    (.incrementAndGet c i)
    (try
      (when (.get closed) (throw (rocks/connection-released-ex)))
      (try (.flushWal ^RocksDB (.-db store) true)
           (catch RocksDBException e (throw (rocks/storage-fault e))))
      (finally (.decrementAndGet c i))))
  nil)
