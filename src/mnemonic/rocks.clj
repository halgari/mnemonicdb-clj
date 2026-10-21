(ns mnemonic.rocks
  "Thin RocksDB wrapper: byte arrays only, no knowledge of datoms.

  Every native call goes through this namespace and is guarded so that no public path
  can touch a closed or freed RocksDB object (which crashes the JVM):

  - Store-level calls (new iterator, get, write, snapshot create/free) run inside the
    store guard: a striped in-flight counter (`Guard`) plus the `closed` flag. A caller
    increments its stripe, re-checks `closed` and only then touches native state;
    `close-store` sets `closed` first and then waits until every stripe is zero. By
    sequential consistency of the atomics either the caller sees `closed` (and backs
    out) or the closer sees the caller's increment (and waits), so no native call can
    start once `close-store` has passed its wait. Unlike the previous
    ReentrantReadWriteLock this is a single uncontended atomic add on a per-thread
    stripe, not a CAS on one word shared by all readers.
    After that every call throws :db.error/connection-released.
  - Iterators live in `Slot`s. A slot serializes its own native calls on its monitor and
    checks its state first; close-store force-closes every registered slot under that
    same monitor before the native db is closed. A Slot is leased to a caller as a
    `RawCursor`; closing the cursor returns the slot to its snapshot's pool (see below)
    or destroys it.
  - Snapshots (Snap) are reference counted: every live slot (leased or idle in the pool)
    holds a reference. `release-snapshot` marks the snapshot released (new cursors then
    throw :db.error/db-released), drains the idle pool, and the native snapshot is freed
    once no slot uses it.

  Iterator pool: creating a RocksDB iterator costs about as much as the seek it serves
  (superversion ref, merging iterator over memtable + L0 files, JNI), so snapshot-bound
  iterators are recycled: each Snap keeps small per-(column family, mode, thread stripe)
  stacks of idle slots sharing the snapshot's two ReadOptions. Idle slots pin their
  superversion; the pool is bounded (`pool-cap` per stack) and drained on release.

  Prefix bloom filters: the four index column families (and their history twins) use a
  fixed-length prefix extractor ([E A] / [A E] / [V A] = 16 bytes; AVET: see
  `avet-prefix-len`) and a prefix-only bloom filter. A cursor whose seek key is at least
  one prefix long and that stays within its seek key (prefix cursors) is opened in
  `prefix mode` (RocksDB default ReadOptions: the bloom filter skips SSTs that cannot
  contain the prefix); every other cursor is opened with total_order_seek. The extractor
  is not part of the on-disk key layout: SST files record the extractor they were built
  with and RocksDB ignores the filter of files built under a different one, so changing
  it needs no format bump (`extractor-version` documents the configuration)."
  (:import (java.lang.ref Cleaner)
           (java.nio.charset StandardCharsets)
           (java.util ArrayList Set)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong AtomicLongArray
                                        AtomicReferenceArray)
           (java.util.concurrent.locks LockSupport)
           (org.rocksdb BlockBasedTableConfig BloomFilter Cache ColumnFamilyDescriptor
                        ColumnFamilyHandle ColumnFamilyOptions CompressionType DBOptions Env
                        FlushOptions IndexType LRUCache Options ReadOptions RocksDB RocksDBException
                        RocksIterator RocksMemEnv Snapshot WriteBatch WriteBufferManager
                        WriteOptions)))

(set! *warn-on-reflection* true)

(def cf-names
  [:default :eavt :aevt :avet :vaet :eavt-h :aevt-h :avet-h :vaet-h :log])

(def ^:private cf-index (zipmap cf-names (range)))

;; ------------------------------------------------------------------ tuning knobs

(defn- prop-bool [^String k default]
  (let [v (System/getProperty k)]
    (if (nil? v) default (not= "false" v))))

(defn- prop-int [^String k default]
  (if-let [v (System/getProperty k)] (Long/parseLong v) default))

(def ^:private pool-enabled?
  "Iterator pool on/off (system property mnemonic.rocks.pool, default on)."
  (prop-bool "mnemonic.rocks.pool" true))

(def ^:private prefix-bloom-default?
  "Prefix bloom filters on/off for newly opened stores (system property
  mnemonic.rocks.prefix-bloom, default on)."
  (prop-bool "mnemonic.rocks.prefix-bloom" true))

(defn- prop-str [^String k default]
  (or (System/getProperty k) default))

;; --- storage configuration (file stores; no on-disk format impact)

(def ^:private default-cache-mb
  "Shared block cache of one file store, in MB (system property mnemonic.rocks.cache-mb;
  per store `:cache-mb`, URI query `cache-mb`). One LRU cache serves all column families
  and holds data, index and filter blocks (off-heap, not part of -Xmx)."
  (prop-int "mnemonic.rocks.cache-mb" 256))

(def ^:private default-write-buffer-mb
  "Budget for all memtables of one file store, in MB (mnemonic.rocks.write-buffer-mb,
  `:write-buffer-mb`, `write-buffer-mb`). Ten column families with 64 MB x 2 memtables
  could otherwise take 1.2 GB of native memory during a bulk load."
  (prop-int "mnemonic.rocks.write-buffer-mb" 128))

(def ^:private default-compression
  "none | lz4 | lz4+zstd (mnemonic.rocks.compression, `:compression`, `compression`).
  Only affects files written from now on; every SST records its own codec."
  (prop-str "mnemonic.rocks.compression" "lz4"))

(def ^:private cache-index-filter?
  (prop-bool "mnemonic.rocks.cache-index-filter" true))

(def ^:private partitioned?
  "Partitioned index and filter blocks (only used with cache-index-filter)."
  (prop-bool "mnemonic.rocks.partitioned" false))

(def ^:private wbm-charge-cache?
  "Charge memtable memory to the block cache (one memory budget) instead of on top of it."
  (prop-bool "mnemonic.rocks.wbm-charge-cache" false))

(def ^:private flush-on-close?
  (prop-bool "mnemonic.rocks.flush-on-close" true))

(def store-option-keys
  "Store options that can be given as URI query parameters (`cache-mb=512`)."
  {"cache-mb" :cache-mb "write-buffer-mb" :write-buffer-mb "compression" :compression})

(defn query-store-opts
  "Store options from a parsed URI query map {\"cache-mb\" \"512\" ...} (string keys);
  throws ex-info :db.error/invalid-db-uri for a bad value. Unknown keys are ignored."
  [kvs]
  (reduce-kv
   (fn [acc k v]
     (if-let [opt (get store-option-keys k)]
       (assoc acc opt
              (if (= opt :compression)
                (if (#{"none" "lz4" "lz4+zstd"} v)
                  v
                  (throw (ex-info (str "Invalid " k " option: " v)
                                  {:db/error :db.error/invalid-db-uri})))
                (try (let [n (Long/parseLong ^String v)]
                       (when-not (pos? n) (throw (NumberFormatException.)))
                       n)
                     (catch NumberFormatException _
                       (throw (ex-info (str "Invalid " k " option: " v " (positive integer expected)")
                                       {:db/error :db.error/invalid-db-uri}))))))
       acc))
   {} kvs))

(def avet-prefix-len
  "Fixed prefix length of the AVET column families: attribute id (8) + value tag (1) +
  the leading 16 value bytes (measured: 17 gave no bloom benefit on string lookup refs
  that share their first 8 characters, 25 and 33 about 1.7x on misses, 41 none because
  typical seek keys are shorter). Seeks shorter than this (short strings, numeric
  values: 17 bytes) use a total-order iterator, which is always correct.
  Set -Dmnemonic.rocks.avet-prefix=N to change it; no format impact."
  (long (prop-int "mnemonic.rocks.avet-prefix" 25)))

(def extractor-version
  "Version of the prefix extractor configuration (lengths per column family). The
  extractor does not affect key layout or format compatibility; bump it with the lengths."
  1)

(def prefix-lens
  "Fixed prefix extractor length per column family that has one."
  {:eavt 16 :aevt 16 :vaet 16 :avet avet-prefix-len
   :eavt-h 16 :aevt-h 16 :vaet-h 16 :avet-h avet-prefix-len})

(def ^:private ^:const pool-stripes 8)
(def ^:private ^:const pool-cap
  "Idle iterators kept per (column family, mode, thread stripe)."
  4)
(def ^:private ^:const n-pools (* (count cf-names) 2 pool-stripes))

(def ^:private ^:const mode-prefix 0)
(def ^:private ^:const mode-total 1)

;; ------------------------------------------------------------------ store

;; guard: Guard (striped in-flight counters); closed: AtomicBoolean;
;; cursors: concurrent set of live Slots; snaps: concurrent set of live Snaps
(defrecord Store [^RocksDB db cfs env closeables guard closed cursors snaps])

(deftype Guard [^AtomicLongArray counts])   ; 16 stripes, 8 longs apart (cache lines)

(defn- new-guard ^Guard [] (->Guard (AtomicLongArray. (* 16 8))))

(defonce ^:private ^Cleaner cleaner (Cleaner/create))

(defn register-cleanup!
  "Run no-arg fn `f` once `obj` becomes phantom-reachable. `f` must not close over `obj`."
  [obj f]
  (.register cleaner obj ^Runnable f)
  nil)

;; ------------------------------------------------------------------ errors

(defn connection-released-ex
  "The error thrown by any operation on a closed store."
  []
  (ex-info "The connection has been released."
           {:db/error :db.error/connection-released
            :cognitect.anomalies/category :cognitect.anomalies/conflict}))

(defn db-released-ex
  "The error thrown when reading a db whose snapshot was released (d/release-db)."
  []
  (ex-info "The database value has been released (d/release-db)."
           {:db/error :db.error/db-released
            :cognitect.anomalies/category :cognitect.anomalies/conflict}))

(defn storage-fault
  "ex-info for a RocksDB error (I/O error, corruption, ...)."
  [^Throwable e]
  (ex-info (str "Storage error: " (.getMessage e))
           {:db/error :db.error/storage-error
            :cognitect.anomalies/category :cognitect.anomalies/fault}
           e))

(defmacro ^:private wrap-rocks
  "Evaluates body, converting a RocksDBException into a :cognitect.anomalies/fault."
  [& body]
  `(try ~@body
        (catch RocksDBException e# (throw (storage-fault e#)))))

;; ------------------------------------------------------------------ store guard

(defn closed?
  "True once `close-store` has started on `store`."
  [^Store store]
  (.get ^AtomicBoolean (.-closed store)))

(defn- thread-id ^long []
  (.threadId (Thread/currentThread)))

(defn- guard-slot ^long []
  (bit-shift-left (bit-and (thread-id) 15) 3))

(defmacro ^:private guarded
  "Runs body inside the store guard (see the ns docstring); throws connection-released
  when the store is closed."
  [store & body]
  `(let [^Store s# ~store
         ^AtomicLongArray c# (.-counts ^Guard (.-guard s#))
         i# (guard-slot)]
     (when (.get ^AtomicBoolean (.-closed s#)) (throw (connection-released-ex)))
     (.incrementAndGet c# i#)
     (try
       (when (.get ^AtomicBoolean (.-closed s#)) (throw (connection-released-ex)))
       ~@body
       (finally (.decrementAndGet c# i#)))))

(defmacro ^:private guarded-or-skip
  "Like `guarded`, but evaluates to nil instead of throwing when the store is closed."
  [store & body]
  `(let [^Store s# ~store
         ^AtomicLongArray c# (.-counts ^Guard (.-guard s#))
         i# (guard-slot)]
     (when-not (.get ^AtomicBoolean (.-closed s#))
       (.incrementAndGet c# i#)
       (try
         (when-not (.get ^AtomicBoolean (.-closed s#))
           ~@body)
         (finally (.decrementAndGet c# i#))))))

(defn- await-quiescent!
  "Blocks until no thread is inside the guard. Called after `closed` was set, so new
  entrants back out immediately."
  [^Store store]
  (let [^AtomicLongArray c (.-counts ^Guard (.-guard store))
        n (.length c)]
    (loop [spins 0]
      (let [busy? (loop [i 0]
                    (cond (>= i n) false
                          (pos? (.get c i)) true
                          :else (recur (inc i))))]
        (when busy?
          (if (< spins 2000)
            (Thread/onSpinWait)
            (LockSupport/parkNanos 100000))
          (recur (inc spins)))))))

(defn- cf-bytes ^bytes [kw]
  (.getBytes ^String (name kw) StandardCharsets/UTF_8))

(defn- close-quietly [^java.lang.AutoCloseable c]
  (when c
    (try (.close c) (catch Throwable _ nil))))

(declare apply-compression! shard-bits)
(defn- shard-bits
  "Cache shards of at least 16 MB: filter blocks of a 64 MB SST are several MB and a block
  larger than a shard cannot be cached (measured: bulk load 6x slower with 64 shards of
  4 MB)."
  ^long [^long cache-bytes]
  (-> (- 63 (Long/numberOfLeadingZeros (max 1 (quot cache-bytes 16777216)))) (max 0) (min 6)))

(defn- apply-compression!
  "L0/L1 uncompressed (short-lived, rewritten soon), LZ4 above, ZSTD at the bottom
  level for \"lz4+zstd\"."
  [^ColumnFamilyOptions o codec]
  (case codec
    "none" (.setCompressionType o CompressionType/NO_COMPRESSION)
    "lz4" (.setCompressionType o CompressionType/LZ4_COMPRESSION)
    "lz4+zstd" (doto o
                 (.setCompressionType CompressionType/LZ4_COMPRESSION)
                 (.setBottommostCompressionType CompressionType/ZSTD_COMPRESSION))))

(defn open-store
  "Open (creating if needed) a store with all column families.
  `mem? true` uses an in-memory RocksMemEnv and treats `path` as a virtual name.
  `prefix-bloom? false` opens without prefix extractors (as stores created before they
  were introduced; also an A/B switch for benchmarks).
  `cache-mb` (shared block cache), `write-buffer-mb` (memtable budget) and `compression`
  (\"none\" | \"lz4\" | \"lz4+zstd\") override the defaults above."
  [{:keys [path mem? prefix-bloom? cache-mb write-buffer-mb compression]}]
  (RocksDB/loadLibrary)
  ;; RocksMemEnv cannot resolve relative names (GetAbsolutePath fails), so virtual
  ;; names are rooted with a leading "/".
  (let [path (if (and mem? (not (.startsWith ^String path "/"))) (str "/" path) path)
        prefix? (if (nil? prefix-bloom?) prefix-bloom-default? (boolean prefix-bloom?))]
  (let [owned (ArrayList.)                       ; closed after db, in reverse order
        own   (fn [x] (.add owned x) x)]
    (try
      (let [env      (when mem? (own (RocksMemEnv. (Env/getDefault))))
            cache-b  (* (long (or cache-mb (if mem? 32 default-cache-mb))) 1048576)
            wbuf-b   (* (long (or write-buffer-mb (if mem? 32 default-write-buffer-mb))) 1048576)
            codec    (or compression default-compression)
            ;; created first, so closed last (after the db and every option object)
            ^LRUCache cache (own (LRUCache. cache-b (int (prop-int "mnemonic.rocks.cache-shard-bits" (shard-bits cache-b))) false 0.3))
            wbm      (own (WriteBufferManager. wbuf-b
                                               ^Cache (if wbm-charge-cache? cache (own (LRUCache. 1048576)))
                                               false))
            dbo      (own (doto (DBOptions.)
                            (.setCreateIfMissing true)
                            (.setCreateMissingColumnFamilies true)
                            (.setWriteBufferManager ^WriteBufferManager wbm)
                            (.setBytesPerSync 1048576)
                            (.setWalBytesPerSync 1048576)
                            (.setMaxBackgroundJobs 4)
                            ;; bounds WAL replay: the oldest memtables are flushed first
                            (.setMaxTotalWalSize (* 2 wbuf-b))))
            _        (when env (.setEnv ^DBOptions dbo ^Env env))
            bloom    (own (BloomFilter. 10.0 false))
            tcfg     (fn [whole-key?]
                       (doto (BlockBasedTableConfig.)
                         (.setBlockCache ^Cache cache)
                         (.setFilterPolicy bloom)
                         (.setWholeKeyFiltering (boolean whole-key?))
                         (.setCacheIndexAndFilterBlocks cache-index-filter?)
                         (.setCacheIndexAndFilterBlocksWithHighPriority true)
                         (.setPinL0FilterAndIndexBlocksInCache true)
                         (.setIndexType (if (and cache-index-filter? partitioned?) IndexType/kTwoLevelIndexSearch IndexType/kBinarySearch))
                         (.setPartitionFilters (boolean (and cache-index-filter? partitioned?)))
                         (.setPinTopLevelIndexAndFilter true)))
            table    (tcfg true)
            ;; index CFs are only read by seek: whole-key filters would be dead weight
            ptable   (tcfg false)
            new-cfo  (fn [^BlockBasedTableConfig tc]
                       (let [^ColumnFamilyOptions o (own (doto (ColumnFamilyOptions.)
                                                           (.setTableFormatConfig ^org.rocksdb.TableFormatConfig tc)))]
                         (apply-compression! o codec)
                         o))
            cfo      (new-cfo table)
            cfo-for  (into {}
                           (map (fn [cf]
                                  [cf (if-let [n (and prefix? (get prefix-lens cf))]
                                        (doto ^ColumnFamilyOptions (new-cfo ptable)
                                          (.useFixedLengthPrefixExtractor (int n)))
                                        cfo)]))
                           cf-names)
            descs    (mapv #(ColumnFamilyDescriptor. (cf-bytes %) ^ColumnFamilyOptions (cfo-for %)) cf-names)
            handles  (ArrayList.)
            db       (wrap-rocks (RocksDB/open dbo ^String path ^java.util.List descs handles))
            cfs      (zipmap cf-names (vec handles))]
        (->Store db cfs env (vec (reverse (vec owned)))
                 (new-guard) (AtomicBoolean. false)
                 (ConcurrentHashMap/newKeySet) (ConcurrentHashMap/newKeySet)))
      (catch Throwable t
        (doseq [c (reverse (vec owned))] (close-quietly c))
        (throw t))))))

(defn destroy-store
  "Delete an on-disk store. Not for mem stores. The store must be closed."
  [^String path]
  (with-open [o (Options.)]
    (wrap-rocks (RocksDB/destroyDB path o)))
  (let [f (java.io.File. path)]
    (when (.isDirectory f)
      (let [^"[Ljava.lang.String;" kids (.list f)]
        (when (or (nil? kids) (zero? (alength kids)))
          (.delete f)))))
  nil)

(defn- cf-handle ^ColumnFamilyHandle [^Store store cf]
  (or (get (:cfs store) cf)
      (throw (ex-info "Unknown column family" {:cf cf}))))

;; ------------------------------------------------------------------ slots and snapshots

(definterface ISnap
  (^org.rocksdb.Snapshot acquire [])
  (^void unref [])
  (^void markReleased [])
  (^boolean isReleased [])
  (^void free []))

;; A native iterator. state: 0 open, 1 destroyed, 2 closed by close-store. Changed only
;; under the slot's monitor, which also serializes every native call on `it`.
;; `own-ro` is set for iterators without a snapshot (they own their ReadOptions).
(deftype Slot [^Store store ^RocksIterator it ^ReadOptions own-ro snap
               ^long cfi ^long mode ^AtomicInteger state])

;; Idle slots of one (cf, mode, thread stripe): a tiny array stack guarded by its own
;; monitor (almost never contended: stripes are per thread). Never hold the pool monitor
;; while taking a slot monitor.
(deftype Pool [^objects arr ^ints cnt])   ; cnt[0] = number of idle slots

(declare drain-pools!)

(deftype Snap [^Store store ^Snapshot native ^AtomicBoolean freed
               ^AtomicLong state   ; bit 0: released; bits 1..: live slot count
               ^ReadOptions ro-p   ; snapshot read options, prefix mode (RocksDB default)
               ^ReadOptions ro-t   ; same, total_order_seek
               ^AtomicReferenceArray pools]
  ISnap
  (acquire [_]
    (loop []
      (let [s (.get state)]
        (when (== 1 (bit-and s 1)) (throw (db-released-ex)))
        (if (.compareAndSet state s (+ s 2))
          native
          (recur)))))
  (unref [this]
    (when (== 1 (.addAndGet state -2))
      (.free this)))
  (markReleased [this]
    (loop []
      (let [s (.get state)]
        (when (zero? (bit-and s 1))
          (if (.compareAndSet state s (bit-or s 1))
            (if (zero? s) (.free this) (drain-pools! this))
            (recur))))))
  (isReleased [_] (== 1 (bit-and (.get state) 1)))
  (free [this]
    ;; After close-store started, close-store frees every registered snapshot.
    (guarded-or-skip store
      (when (.compareAndSet freed false true)
        (.remove ^Set (.-snaps store) this)
        (.close ro-p)
        (.close ro-t)
        (.releaseSnapshot ^RocksDB (.-db store) native)))))

(defn snapshot
  "A reference-counted snapshot of `store` (see `release-snapshot`)."
  ^mnemonic.rocks.Snap [^Store store]
  (guarded store
    (let [native (.getSnapshot ^RocksDB (.-db store))
          s (->Snap store native (AtomicBoolean. false) (AtomicLong. 0)
                    (doto (ReadOptions.) (.setSnapshot native))
                    (doto (ReadOptions.) (.setSnapshot native) (.setTotalOrderSeek true))
                    (AtomicReferenceArray. (int n-pools)))]
      (.add ^Set (.-snaps store) s)
      s)))

(defn release-snapshot
  "Marks `snap` released: new reads through it throw :db.error/db-released; idle pooled
  iterators are closed and the native snapshot is freed once no open cursor uses it.
  Idempotent, safe after close-store."
  [_store ^Snap snap]
  (.markReleased snap)
  nil)

(defn snapshot-released? [^Snap snap] (.isReleased snap))

;; ------------------------------------------------------------------ slot lifecycle

(defn- destroy-slot!
  "Closes the native iterator of `s` (unless close-store did) and drops its snapshot ref."
  [^Slot s]
  (let [done? (locking s
                (when (.compareAndSet ^AtomicInteger (.-state s) 0 1)
                  (.close ^RocksIterator (.-it s))
                  (when-let [ro (.-own-ro s)] (.close ^ReadOptions ro))
                  true))]
    (when done?
      (.remove ^Set (.-cursors ^Store (.-store s)) s)
      (when-let [sn (.-snap s)] (.unref ^ISnap sn)))
    nil))

(defn- force-close-slot!
  "close-store: close the native iterator without touching the snapshot."
  [^Slot s]
  (locking s
    (when (.compareAndSet ^AtomicInteger (.-state s) 0 2)
      (.close ^RocksIterator (.-it s))
      (when-let [ro (.-own-ro s)] (.close ^ReadOptions ro))))
  nil)

(defn- pool-of
  "The idle-slot stack for (cfi, mode) of the calling thread's stripe, created lazily."
  ^Pool [^Snap snap ^long cfi ^long mode]
  (let [^AtomicReferenceArray pools (.-pools snap)
        i (int (+ (* (+ (* cfi 2) mode) pool-stripes) (bit-and (thread-id) (dec pool-stripes))))]
    (or (.get pools i)
        (let [p (Pool. (object-array pool-cap) (int-array 1))]
          (if (.compareAndSet pools i nil p) p (.get pools i))))))

(defn- pool-pop!
  "An idle slot of `p`, or nil."
  [^Pool p]
  (locking p
    (let [^ints c (.-cnt p)
          n (aget c 0)]
      (when (pos? n)
        (let [^objects a (.-arr p)
              i (dec n)
              s (aget a i)]
          (aset a i nil)
          (aset c 0 i)
          s)))))

(defn- pool-push!
  "Pushes `s`; false when `p` is full."
  [^Pool p s]
  (locking p
    (let [^ints c (.-cnt p)
          n (aget c 0)]
      (when (< n pool-cap)
        (aset ^objects (.-arr p) n s)
        (aset c 0 (unchecked-inc-int n))
        true))))

(defn- drain-pool!
  "Destroys every idle slot of `p`. Slots are taken out under the pool monitor and
  destroyed outside it."
  [^Pool p]
  (loop []
    (when-let [s (pool-pop! p)]
      (destroy-slot! s)
      (recur))))

(defn- drain-pools!
  "Destroys every idle slot of `snap`'s pools."
  [^Snap snap]
  (let [^AtomicReferenceArray pools (.-pools snap)]
    (dotimes [i (.length pools)]
      (when-let [p (.get pools i)]
        (drain-pool! p))))
  nil)

(defn retire-snapshot!
  "Closes the idle pooled iterators of `snap` WITHOUT releasing it: the snapshot and every
  open cursor stay valid, later reads simply open (and pool) fresh iterators. Called for
  superseded published dbs so that their idle iterators do not pin memtables/SSTs until
  the GC runs the Cleaner."
  [_store ^Snap snap]
  (drain-pools! snap))

(defn- give-back!
  "A leased slot is no longer used: pool it, or destroy it when it cannot be reused.
  Must not be called holding any slot monitor."
  [^Slot s]
  (let [snap (.-snap s)]
    (if (and snap pool-enabled?
             (zero? (.get ^AtomicInteger (.-state s)))
             (not (.isReleased ^ISnap snap))
             (not (closed? (.-store s))))
      (let [^Pool p (pool-of snap (.-cfi s) (.-mode s))]
        (if (pool-push! p s)
          ;; the snapshot may have been released after the check above, with its
          ;; drain already past this stack: drain again (release sets the flag
          ;; before draining, we push before re-reading it)
          (when (.isReleased ^ISnap snap) (drain-pools! snap))
          (destroy-slot! s)))
      (destroy-slot! s))))

(defn- new-slot
  ^Slot [^Store store cf cfi mode snap]
  (guarded store
    (let [mode (long mode)
          _ (when snap (.acquire ^ISnap snap))        ; throws db-released
          own-ro (when-not snap
                   (let [ro (ReadOptions.)]
                     (when (== mode mode-total) (.setTotalOrderSeek ro true))
                     ro))
          ro (or own-ro (if (== mode mode-prefix) (.-ro-p ^Snap snap) (.-ro-t ^Snap snap)))]
      (try
        (let [s (->Slot store (.newIterator ^RocksDB (.-db store) (cf-handle store cf) ^ReadOptions ro)
                        own-ro snap cfi mode (AtomicInteger. 0))]
          (.add ^Set (.-cursors store) s)
          s)
        (catch Throwable t
          (when own-ro (.close ^ReadOptions own-ro))
          (when snap (.unref ^ISnap snap))
          (throw t))))))

(defn- borrow-slot
  ^Slot [^Store store cf cfi mode snap]
  (if (and snap pool-enabled?)
    (do
      (when (closed? store) (throw (connection-released-ex)))
      (when (.isReleased ^ISnap snap) (throw (db-released-ex)))
      (let [^Pool p (pool-of snap (long cfi) (long mode))]
        (loop []
          (if-let [^Slot s (pool-pop! p)]
            (if (zero? (.get ^AtomicInteger (.-state s)))
              s
              (recur))
            (new-slot store cf cfi mode snap)))))
    (new-slot store cf cfi mode snap)))

;; ------------------------------------------------------------------ point ops

(defn get-bytes
  "Point lookup. `snap` (a Snap) may be nil."
  ^bytes [^Store store cf snap ^bytes k]
  (guarded store
    (let [^RocksDB db (.-db store)
          h (cf-handle store cf)]
      (if snap
        (let [^Snap snap snap
              _ (.acquire snap)]
          (try
            (wrap-rocks (.get db h ^ReadOptions (.-ro-t snap) k))
            (finally
              ;; unref may free, which re-enters the guard (nests)
              (.unref snap))))
        (wrap-rocks (.get db h k))))))

(defn write!
  "Atomically apply `ops`: seq of [:put cf k v] / [:delete cf k]."
  [^Store store ops {:keys [sync?]}]
  (with-open [wb (WriteBatch.)
              ;; mem stores are not durable: no WAL (a synced write needs the WAL)
              wo (let [mem? (some? (.-env store))]
                   (doto (WriteOptions.)
                     (.setDisableWAL mem?)
                     (.setSync (and (boolean sync?) (not mem?)))))]
    ;; the batch references column family handles: build it under the guard too
    (guarded store
      (doseq [op ops]
        (case (nth op 0)
          :put    (.put wb (cf-handle store (nth op 1)) ^bytes (nth op 2) ^bytes (nth op 3))
          :delete (.delete wb (cf-handle store (nth op 1)) ^bytes (nth op 2))))
      (wrap-rocks (.write ^RocksDB (.-db store) wo wb))))
  nil)

(defn flush!
  "Flushes the memtables of every column family to SST files (waits)."
  [^Store store]
  (guarded store
    (with-open [fo (doto (FlushOptions.) (.setWaitForFlush true))]
      (wrap-rocks (.flush ^RocksDB (.-db store) fo ^java.util.List (mapv #(cf-handle store %) cf-names)))))
  nil)

(defn compact!
  "Full manual compaction of every column family."
  [^Store store]
  (guarded store
    (doseq [cf cf-names]
      (wrap-rocks (.compactRange ^RocksDB (.-db store) (cf-handle store cf)))))
  nil)

;; ------------------------------------------------------------------ cursors

(definterface IRawCursor
  (seek [^bytes k])
  (seekToFirst [])
  (seekToLast [])
  (seekForPrev [^bytes k])
  (^boolean isValid [])
  (next [])
  (prev [])
  (^bytes key [])
  (^bytes value [])
  (^bytes seekKey [^bytes k])
  (^bytes nextKey [])
  (^void close []))

(definterface ILease
  (^boolean markDone []))

(defn- check-status
  "Called when the iterator became invalid: an I/O error or corruption must not look
  like the end of the data."
  [^RocksIterator it]
  (wrap-rocks (.status it)))

(defn- throw-closed [state]
  (if (== (long state) 2)
    (throw (connection-released-ex))
    (throw (ex-info "Cursor is closed" {:db/error :db.error/cursor-closed
                                        :cognitect.anomalies/category :cognitect.anomalies/incorrect}))))

(defmacro ^:private open!
  "Inside (locking slot): throws unless this lease and its slot are open."
  [slot done]
  `(let [st# (.get ^AtomicInteger (.-state ~slot))]
     (when (or ~done (not (zero? st#)))
       (throw-closed (if ~done 1 st#)))))

;; A lease on a Slot. All fields and the slot state are guarded by the slot's monitor.
;; `seeked` is false until the first positioning call: a recycled iterator still has the
;; position of its previous user and must look fresh (invalid).
(deftype RawCursor [^Slot slot
                    ^:unsynchronized-mutable ^boolean done
                    ^:unsynchronized-mutable ^boolean seeked]
  IRawCursor
  (seek [_ k]
    (let [s slot]
      (locking s (open! s done) (set! seeked (boolean true)) (.seek ^RocksIterator (.-it s) ^bytes k) nil)))
  (seekToFirst [_]
    (let [s slot]
      (locking s (open! s done) (set! seeked (boolean true)) (.seekToFirst ^RocksIterator (.-it s)) nil)))
  (seekToLast [_]
    (let [s slot]
      (locking s (open! s done) (set! seeked (boolean true)) (.seekToLast ^RocksIterator (.-it s)) nil)))
  (seekForPrev [_ k]
    (let [s slot]
      (locking s (open! s done) (set! seeked (boolean true)) (.seekForPrev ^RocksIterator (.-it s) ^bytes k) nil)))
  (isValid [_]
    (let [s slot]
      (locking s
        (open! s done)
        (let [^RocksIterator it (.-it s)]
          (and seeked (or (.isValid it) (do (check-status it) false)))))))
  (next [_] (let [s slot] (locking s (open! s done) (.next ^RocksIterator (.-it s)) nil)))
  (prev [_] (let [s slot] (locking s (open! s done) (.prev ^RocksIterator (.-it s)) nil)))
  (key [_] (let [s slot] (locking s (open! s done) (.key ^RocksIterator (.-it s)))))
  (value [_] (let [s slot] (locking s (open! s done) (.value ^RocksIterator (.-it s)))))
  (seekKey [_ k]
    (let [s slot]
      (locking s
        (open! s done)
        (set! seeked (boolean true))
        (let [^RocksIterator it (.-it s)]
          (.seek it ^bytes k)
          (if (.isValid it) (.key it) (do (check-status it) nil))))))
  (nextKey [_]
    (let [s slot]
      (locking s
        (open! s done)
        (let [^RocksIterator it (.-it s)]
          (.next it)
          (if (.isValid it) (.key it) (do (check-status it) nil))))))
  (close [this]
    ;; give-back! may lock other (idle) slots while draining: never under the monitor
    (when (.markDone this) (give-back! slot))
    nil)
  ILease
  (markDone [_]
    (let [s slot]
      (locking s
        (if done
          false
          (do (set! done (boolean true)) true)))))
  java.lang.AutoCloseable)

(defn- prefix-mode?
  "True when a cursor over `cf` whose seek key is `prefix` (and that never leaves it) may
  use the prefix bloom filter: the seek key spans at least one extractor prefix."
  [cf ^bytes prefix]
  (and prefix
       (when-let [n (get prefix-lens cf)]
         (>= (alength prefix) (long n)))))

(defn raw-cursor
  "New cursor over `cf`, reading from `snap` (a Snap, may be nil). Caller must close it.
  `prefix` (optional): the key prefix the caller will seek to and never read beyond
  (a prefix cursor); lets RocksDB use the prefix bloom filter when it is at least as
  long as the column family's extractor prefix. Without it the cursor is total-order.
  Returns a RawCursor (hint call sites with ^mnemonic.rocks.IRawCursor)."
  (^mnemonic.rocks.RawCursor [store cf snap] (raw-cursor store cf snap nil))
  (^mnemonic.rocks.RawCursor [^Store store cf snap ^bytes prefix]
   (let [mode (if (and prefix (prefix-mode? cf prefix)) mode-prefix mode-total)
         cfi (long (cf-index cf))]
     (->RawCursor (borrow-slot store cf cfi mode snap) false false))))

;; ------------------------------------------------------------------ close

(defn close-store
  "Closes `store`: waits for in-flight native calls, then closes every open cursor and
  frees every snapshot, then closes handles, the db and options. Afterwards every
  operation on the store, its cursors and its snapshots throws
  :db.error/connection-released. Idempotent."
  [^Store store]
  (when (.compareAndSet ^AtomicBoolean (.-closed store) false true)
    (await-quiescent! store)
    (doseq [^Slot s (vec (.-cursors store))]
      (try (force-close-slot! s) (catch Throwable _ nil)))
    (.clear ^Set (.-cursors store))
    (doseq [^Snap s (vec (.-snaps store))]
      (when (.compareAndSet ^AtomicBoolean (.-freed s) false true)
        (close-quietly (.-ro-p s))
        (close-quietly (.-ro-t s))
        (try (.releaseSnapshot ^RocksDB (.-db store) ^Snapshot (.-native s))
             (catch Throwable _ nil))))
    (.clear ^Set (.-snaps store))
    ;; Flush the memtables so the next open does not replay the WAL (replay of a large
    ;; WAL dominated reopen time). No other native call can be in flight here (guard
    ;; quiesced, cursors and snapshots closed). Failure is harmless: the WAL still holds
    ;; the data and is replayed.
    (when (and flush-on-close? (nil? (:env store)))
      (try (with-open [fo (doto (FlushOptions.) (.setWaitForFlush true))]
             (.flush ^RocksDB (.-db store) fo ^java.util.List (mapv #(get (:cfs store) %) cf-names)))
           (catch Throwable _ nil)))
    (doseq [^ColumnFamilyHandle h (vals (:cfs store))] (close-quietly h))
    (close-quietly (.-db store))
    (doseq [c (:closeables store)] (close-quietly c)))
  nil)
