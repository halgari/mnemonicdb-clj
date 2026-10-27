(ns mnemonic.segcache
  "In-heap cache of decoded index segments, shared by every db value of one store (the
  way a Datomic peer caches immutable index segments). Pure heap: it never touches
  RocksDB itself; fills are read by the caller through the store's guarded cursors.

  Granules (the unit that is cached, filled and invalidated), per column family, current
  and history alike:
    EAVT  [e]            8 key bytes: every datom of one entity
    VAET  [v]            8 key bytes: every reference to one entity
    AVET  [a v]          a complete value of a prefix-free encoding (incl. empty = a
                         cached negative lookup)
    AEVT  [a, e >>> 10]  a chunk of 1024 consecutive entity ids of one attribute
  Longer prefixes ([e a], [e a v], [v a], [a v e], [a e]) are served by slicing their
  granule. A segment holds the keys (and, for history CFs, the retracting txs) plus a
  lazily decoded Datom array.

  Validity (slot stamps, no invalidation messages): `stripes` is a fixed array of longs.
  `before-write!` runs in rocks/write! BEFORE every batch and stamps, for each put/delete
  in an index CF, stripes[hash(cf, granule)] = t (the t of the batch's :log entry). A
  segment read from the snapshot of a db with basis R records lo = the stamp read after
  the snapshot existed (kept only when lo <= R): no write touched the granule in (lo, R].
  It is valid for a db with basis b iff
    lo <= b <= hi                      (hi starts at R), or
    b > hi and the stamp is still <= hi (nothing written since; hi is raised to b).
  Stamps only grow and a collision only costs a miss, so a stale segment is never served.
  The stamp is written before the batch, and any db with basis >= t is published after
  the batch (atom swap / future completion: happens-before), so a reader holding such a
  db sees the stamp. A segment superseded by a newer fill of the same granule stays
  attached to it (one level) and keeps serving older db values in its own [lo, hi].

  Memory-ordering assumptions (what the validity rule relies on):
  - One writer at a time stamps: regular batches stamp under the cache's lock, each with
    a t above every earlier stamp, so plain ordered stores (lazySet) are the max. Other
    concurrent writers of index CFs (t not above the last) are irregular (epoch below).
  - Writer: stamp -> full fence -> RocksDB write. Any snapshot that contains the batch
    was taken after the write, hence after the stamps.
  - Reader: snapshot -> full fence (`snapshot-taken!`, in db/base-db and successor-db) ->
    stamp reads, so a stamp read cannot be ordered before the snapshot it validates.
    RocksDB's own snapshot/write synchronization (C++ acquire/release around the
    sequence number) is what makes \"the snapshot contains the batch\" a happens-after of
    the batch's write.
  - Db values reach other threads by safe publication (the connection's atom, futures),
    which also carries the stamps written before them.
  - Segments are immutable once published through the map (ConcurrentHashMap gives the
    happens-before); lo/hi/prev/state are volatile and only widen or retire. Decoded
    Datoms are stored back racily: their fields are final, and mutable values (Date,
    byte[]) are copied on the way out (mnemonic.index/seg-datom).
  - Closing: close-store waits for in-flight native calls, then `close!` drops the
    segments and refuses later fills; hits check the store and snapshot first.

  The basis of a db identifies its index contents only when writes come in t order with a
  :log entry (the transactor). Any other batch that touches an index CF (raw test writes,
  a retried t) is irregular: it moves `epoch` to odd before the write and to even after;
  a db records the epoch around taking its snapshot (-1, uncacheable, if it moved or was
  odd) and a segment is only used by dbs of the epoch it was filled in.

  Memory: two CLOCK pools with byte budgets (estimated ~130 B per datom): points (entity,
  reverse refs, [a v] lookups) get 3/4, AEVT scan chunks 1/4, so a large scan never
  evicts hot entities. Admission on the second miss by a later read (a sliding Bloom
  doorkeeper, see `admit?`): one-off reads do not pay for copying and storing a granule,
  and a working set much larger than the budget does not churn it. Granules larger than `point-cap` /
  `chunk-cap` entries are remembered as 'big' and streamed from RocksDB."
  (:import (java.util Arrays)
           (java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue)
           (java.util.concurrent.atomic AtomicLong AtomicLongArray
                                        AtomicReference LongAdder)
           (java.util.concurrent.locks ReentrantLock)
           (java.util.function BiFunction)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ configuration

(defn- prop-long ^long [^String k ^long default]
  (if-let [v (System/getProperty k)] (Long/parseLong v) default))

(defn default-mb
  "Default budget in MB (system property mnemonic.index-cache-mb, default 256; 0 = off)."
  ^long []
  (prop-long "mnemonic.index-cache-mb" 256))

;; ------------------------------------------------------------------ keys and hashes

(defn cf-code
  "Small code of an index column family (0 = not cached)."
  ^long [cf]
  (case cf :eavt 1 :vaet 2 :avet 3 :aevt 4 :eavt-h 5 :vaet-h 6 :avet-h 7 :aevt-h 8 0))

(def ^:private ^:const fnv-offset -3750763034362895579)
(def ^:private ^:const fnv-prime 1099511628211)

(defmacro ^:private fnv [h b]
  `(unchecked-multiply (bit-xor ~h (bit-and 0xff (long ~b))) fnv-prime))

(defn- fmix
  "murmur3 fmix64 (a 31-polynomial or plain FNV leaves consecutive ids clustered)."
  ^long [^long h]
  (let [h (bit-xor h (unsigned-bit-shift-right h 33))
        h (unchecked-multiply h -49064778989728563)
        h (bit-xor h (unsigned-bit-shift-right h 33))
        h (unchecked-multiply h -4265267296055464877)]
    (bit-xor h (unsigned-bit-shift-right h 33))))

(defn hash-range
  "Hash of cf code `cfc` and the first `n` bytes of `b`."
  ^long [^long cfc ^bytes b ^long n]
  (loop [i 0 h (bit-xor fnv-offset cfc)]
    (if (< i n)
      (recur (inc i) (fnv h (aget b i)))
      (fmix h))))

(def ^:const chunk-bits
  "AEVT granules are chunks of 2^chunk-bits consecutive entity ids of one attribute."
  10)

(def ^:const max-chunk (dec (bit-shift-left 1 (- 64 chunk-bits))))

(defn raw-long
  "The 8 bytes at `off` of `b` as a raw big-endian long (no sign flip)."
  ^long [^bytes b ^long off]
  (loop [i 0 acc 0]
    (if (< i 8)
      (recur (inc i) (bit-or (bit-shift-left acc 8) (bit-and 0xff (long (aget b (+ off i))))))
      acc)))

(defn key-chunk
  "AEVT chunk number of a key (or prefix of at least 16 bytes): raw E >>> chunk-bits."
  ^long [^bytes k]
  (unsigned-bit-shift-right (raw-long k 8) chunk-bits))

(defn hash-chunk
  "Same as `hash-range` over the 16-byte chunk granule of `k`'s attribute and `chunk`,
  without building it."
  ^long [^long cfc ^bytes k ^long chunk]
  (let [h (loop [i 0 h (bit-xor fnv-offset cfc)]
            (if (< i 8) (recur (inc i) (fnv h (aget k i))) h))]
    (loop [i 0 h h]
      (if (< i 8)
        (recur (inc i) (fnv h (unsigned-bit-shift-right chunk (* 8 (- 7 i)))))
        (fmix h)))))

(defn- put-raw-long! [^bytes b ^long off ^long x]
  (dotimes [i 8]
    (aset b (+ off i) (unchecked-byte (unsigned-bit-shift-right x (* 8 (- 7 i)))))))

(defn chunk-granule
  "The 16-byte granule [A, chunk] for the attribute of `k` (its first 8 bytes)."
  ^bytes [^bytes k ^long chunk]
  (let [g (byte-array 16)]
    (System/arraycopy k 0 g 0 8)
    (put-raw-long! g 8 chunk)
    g))

(defn chunk-start
  "The smallest key of `chunk` of the attribute of `k`."
  ^bytes [^bytes k ^long chunk]
  (let [g (byte-array 16)]
    (System/arraycopy k 0 g 0 8)
    (put-raw-long! g 8 (bit-shift-left chunk chunk-bits))
    g))

(defn- escaped-end
  "End of an escaped, 0x00 0x00 terminated string starting at `i`, or -1."
  ^long [^bytes b ^long i ^long n]
  (loop [i i]
    (if (>= (inc i) n)
      -1
      (if (zero? (aget b i))
        (case (bit-and 0xff (long (aget b (inc i))))
          0x00 (+ i 2)
          0xff (recur (+ i 2))
          -1)
        (recur (inc i))))))

(defn value-end
  "End offset of the complete value encoding at `off` within the first `n` bytes of `b`,
  or -1 when it is incomplete or of a type not handled here. Only encodings that are
  prefix-free (fixed width, terminated or length-prefixed) are accepted, so an AVET key
  that starts with [a v] has exactly [a v] as its attribute-value part."
  ^long [^bytes b ^long off ^long n]
  (if (>= off n)
    -1
    (let [i (inc off)
          tag (bit-and 0xff (long (aget b off)))
          w (case tag 0x10 1 0x11 8 0x13 4 0x14 8 0x16 8 0x17 16 -1)]
      (if (>= w 0)
        (let [e (+ i w)] (if (<= e n) e -1))
      (case tag
        (0x18 0x1b) (escaped-end b i n)
        (0x19 0x1a) (let [j (escaped-end b i n)] (if (neg? j) -1 (escaped-end b j n)))
        0x1c (if (<= (+ i 4) n)
               (let [len (bit-or (bit-shift-left (bit-and 0xff (long (aget b i))) 24)
                                 (bit-shift-left (bit-and 0xff (long (aget b (+ i 1)))) 16)
                                 (bit-shift-left (bit-and 0xff (long (aget b (+ i 2)))) 8)
                                 (bit-and 0xff (long (aget b (+ i 3)))))
                     e (+ i 4 len)]
                 (if (<= e n) e -1))
               -1)
        0x1d (loop [j i]
               (cond
                 (>= j n) -1
                 (zero? (aget b j)) (inc j)
                 (== 1 (aget b j)) (recur (inc j))          ; nil element
                 :else (let [e (value-end b j n)] (if (neg? e) -1 (recur e)))))
        -1)))))

(defn- write-granule-len
  "Length of the granule of index key `k` (of cf code `cfc`), or -1 (no granule)."
  ^long [^long cfc ^bytes k]
  (let [n (alength k)]
    (case cfc
      (1 2 5 6) (if (>= n 8) 8 -1)
      (3 7) (if (>= n 24) (- n 16) -1)
      -1)))

;; Granule key in a pool map. A stored key owns an exact copy; a probe wraps the caller's
;; prefix bytes and a length, so a lookup copies nothing.
(deftype GKey [^long cfc ^bytes g ^long len ^long h]
  Object
  (hashCode [_] (unchecked-int h))
  (equals [_ o]
    (and (instance? GKey o)
         (let [^GKey o o]
           (and (== cfc (.-cfc o))
                (== len (.-len o))
                (Arrays/equals g 0 (unchecked-int len) ^bytes (.-g o) 0 (unchecked-int len)))))))

;; ------------------------------------------------------------------ segments

(definterface ISeg
  (^long lo [])
  (^void lowerLo [^long x])
  (^long hi [])
  (^void raiseHi [^long x])
  (prev [])
  (^void setPrev [p])
  (^boolean refd [])
  (^void setRefd [^boolean b])
  (^long state [])
  (^void setState [^long s]))

;; state: 0 top entry of its key, 1 attached as an older version (prev) or never
;; published as top, 2 removed. lo/hi only widen. A segment's arrays are immutable except
;; the lazy Datom slots in `ds` (racy but benign: decoding is deterministic and a Datom's
;; fields are final).
(deftype Seg [gkey ^objects ks ^objects ds ^longs rtx ^long epoch ^boolean big ^long weight
              ^boolean mut   ; holds mutable values (Date, byte[], tuples): detach on read
              ^:volatile-mutable ^long lo*
              ^:volatile-mutable ^long hi*
              ^:volatile-mutable prev*
              ^:volatile-mutable ^boolean refd*
              ^:volatile-mutable ^long state*]
  ISeg
  (lo [_] lo*)
  (lowerLo [_ x] (when (< x lo*) (set! lo* x)))
  (hi [_] hi*)
  (raiseHi [_ x] (when (> x hi*) (set! hi* x)))
  (prev [_] prev*)
  (setPrev [_ p] (set! prev* p))
  (refd [_] refd*)
  (setRefd [_ b] (set! refd* b))
  (state [_] state*)
  (setState [_ s] (set! state* s)))

(defn seg-keys ^objects [^Seg s] (.-ks s))
(defn seg-datoms ^objects [^Seg s] (.-ds s))
(defn seg-rtx ^longs [^Seg s] (.-rtx s))
(defn seg-big? [^Seg s] (.-big s))
(defn seg-mut? [^Seg s] (.-mut s))
(defn seg-size ^long [^Seg s] (alength ^objects (.-ks s)))
(defn seg-weight-of ^long [^Seg s] (.-weight s))

(defn- total-weight ^long [^Seg s]
  (let [^Seg p (.prev s)]
    (+ (.-weight s) (if p (.-weight p) 0))))

(defn- align8 ^long [^long n] (bit-and (+ n 7) -8))

(defn- value-weight
  "Estimated heap bytes of the decoded value of index key `k` (cf code `cfc`): what the
  Datom's value object adds on top of the key bytes (interned keywords and Boolean
  constants add nothing)."
  ^long [^long cfc ^bytes k]
  (let [n (alength k)]
    (case cfc
      (2 6) 16                                          ; VAET: the ref as a boxed Long
      (let [[off end] (if (or (== cfc 3) (== cfc 7)) [8 (- n 16)] [16 (- n 8)])
            off (long off)
            vlen (- (long end) off)]
        (if (or (neg? vlen) (>= off n))
          16
          (case (bit-and 0xff (long (aget k off)))
            0x10 0
            (0x11 0x13 0x14) 16
            0x16 24
            0x17 32
            0x19 0
            (0x18 0x1a 0x1b) (+ 24 (align8 (+ 16 vlen)))      ; String: object + byte[]
            0x1c (align8 (+ 16 vlen))
            0x1d (+ 48 (* 2 (align8 (+ 16 vlen))))            ; tuple: vector + elements
            (+ 40 (align8 (+ 16 vlen)))))))))

(defn- seg-weight
  "Estimated heap bytes of a segment once its Datoms are decoded: per entry the key
  byte[] (16 + length, 8-aligned), two array slots, the Datom (48), its decoded value
  (`value-weight`) and, for history, a long; plus the segment, map node and granule key.
  Calibrated against measured heap for longs, short and long strings (segcache-unit-test)."
  [cfc ^bytes g ^objects ks rtx big?]
  (if big?
    (+ 160 (alength g))
    (loop [i 0 acc (+ 160 (alength g))]
      (if (< i (alength ks))
        (let [^bytes k (aget ks i)]
          (recur (inc i) (+ acc (align8 (+ 16 (alength k))) 8 48 (value-weight (long cfc) k)
                            (if rtx 8 0))))
        acc))))

;; ------------------------------------------------------------------ pools

(defn gkey
  "Granule key of cf code `cfc` over the first `n` bytes of `b` (not copied), hash `h`."
  ^GKey [^long cfc ^bytes b ^long n]
  (GKey. cfc b n (hash-range cfc b n)))

(defn gkey-hash ^long [^GKey k] (.-h k))

;; What a lookup needs to know about the reading db (implemented by mnemonic.db/Base).
(definterface IReader
  (^long basisT [])
  (^long epoch []))

(deftype Gen [^ConcurrentHashMap m ^ConcurrentLinkedQueue q ^AtomicLong weight])

(defn- new-gen ^Gen [] (Gen. (ConcurrentHashMap. 1024) (ConcurrentLinkedQueue.) (AtomicLong. 0)))

(deftype Pool [^AtomicReference gen ^long budget ^ReentrantLock lock ^LongAdder evictions])

(defn- new-pool ^Pool [^long budget]
  (Pool. (AtomicReference. (new-gen)) budget (ReentrantLock.) (LongAdder.)))

(definterface IDoor
  (^boolean seenOrAdd [^long h])
  (^void reset []))

(defn- bit-set? [^longs a ^long b]
  (not (zero? (bit-and (aget a (unsigned-bit-shift-right b 6)) (bit-shift-left 1 (bit-and b 63))))))

(defn- set-bit! [^longs a ^long b]
  (let [w (unsigned-bit-shift-right b 6)]
    (aset a w (bit-or (aget a w) (bit-shift-left 1 (bit-and b 63))))))

;; Doorkeeper: two Bloom filters (k = 2) over the hashes of missed granules, the current
;; and the previous window of `window` first misses, so a granule seen once in the last
;; 1-2 windows counts as seen. A Bloom filter has no false negatives; a false positive
;; only admits a granule early. Racy plain bit writes: a lost bit costs one admission.
(deftype Door [^long mask ^long window ^AtomicLong n
               ^AtomicReference gens]   ; object-array [current previous] of long[] bits
  IDoor
  (seenOrAdd [_ h]
    (let [b1 (bit-and mask (unsigned-bit-shift-right h 20))
          b2 (bit-and mask (unsigned-bit-shift-right (unchecked-multiply h -7046029254386353131) 24))
          ^objects g (.get gens)
          ^longs c (aget g 0)
          ^longs p (aget g 1)]
      (if (or (and (bit-set? c b1) (bit-set? c b2))
              (and (bit-set? p b1) (bit-set? p b2)))
        true
        (do (set-bit! c b1)
            (set-bit! c b2)
            (when (zero? (rem (.incrementAndGet n) window))
              (.compareAndSet gens g (object-array [(long-array (alength c)) c])))
            false))))
  (reset [_]
    (let [^objects g (.get gens)
          w (alength ^longs (aget g 0))]
      (.set gens (object-array [(long-array w) (long-array w)])))))

(deftype Cache [^AtomicLongArray stripes ^long smask
                ^Door door ^boolean door?
                ^Pool point ^Pool scan
                ^AtomicLong epoch* ^AtomicLong max-t
                ^long point-cap ^long chunk-cap ^long budget
                ^LongAdder hits ^LongAdder misses ^LongAdder stale
                ^LongAdder fills ^LongAdder bigs
                ^java.util.Set big-scans   ; hashes of [a] scans larger than the scan budget
                ;; presence bits: set for every stored granule hash, never cleared on
                ;; eviction; a clear bit proves the granule is not cached, so a miss skips
                ;; the map probe. Racy plain writes: a lost bit only costs a miss.
                ^longs present ^long pmask
                ^java.util.concurrent.atomic.AtomicBoolean closed])   ; store closed: no fills

(defn point-pool ^Pool [^Cache c] (.-point c))
(defn scan-pool ^Pool [^Cache c] (.-scan c))
(defn point-cap ^long [^Cache c] (.-point-cap c))
(defn chunk-cap ^long [^Cache c] (.-chunk-cap c))

(defn scan-budget ^long [^Cache c] (.-budget ^Pool (.-scan c)))
(defn point-budget ^long [^Cache c] (.-budget ^Pool (.-point c)))
(defn point-weight
  "Estimated bytes the point pool holds now."
  ^long [^Cache c]
  (.get ^AtomicLong (.-weight ^Gen (.get ^AtomicReference (.-gen ^Pool (.-point c))))))

(defn big-scan?
  "True when the attribute scan of hash `h` was found larger than half the scan budget
  (it is then streamed from RocksDB: caching it would only churn the scan pool)."
  [^Cache c ^long h]
  (.contains ^java.util.Set (.-big-scans c) h))

(defn mark-big-scan! [^Cache c ^long h]
  (.add ^java.util.Set (.-big-scans c) h)
  nil)

(defn- pow2-at-least ^long [^long x]
  (loop [p 1] (if (< p x) (recur (bit-shift-left p 1)) p)))

(defn new-cache
  "A cache with a budget of `mb` megabytes, or nil when `mb` is not positive. Test and
  tuning knobs (system properties, read here): mnemonic.index-cache.stripe-bits (18),
  .point-cap (512 entries), .chunk-cap (16384), .doorkeeper (true), .budget-kb (tests:
  overrides `mb`)."
  [mb]
  (let [mb (long (or mb (default-mb)))]
    (when (pos? mb)
      (let [budget (if-let [kb (System/getProperty "mnemonic.index-cache.budget-kb")]
                     (* 1024 (Long/parseLong kb))
                     (* mb 1048576))
            sbits (prop-long "mnemonic.index-cache.stripe-bits" 18)
            ;; a granule is admitted when it misses again within 1-2 windows of first
            ;; misses; a window is about the number of segments the budget holds
            window (max 4096 (quot budget 512))
            dbits (pow2-at-least (* 8 window))
            ;; ~8 bits per segment the budget holds (64k..32M bits)
            pbits (min (bit-shift-left 1 25) (max 65536 (pow2-at-least (quot budget 64))))]
        (Cache. (AtomicLongArray. (unchecked-int (bit-shift-left 1 sbits))) (dec (bit-shift-left 1 sbits))
                (Door. (dec dbits) window (AtomicLong. 0)
                       (AtomicReference. (object-array [(long-array (quot dbits 64))
                                                        (long-array (quot dbits 64))])))
                (not= "false" (System/getProperty "mnemonic.index-cache.doorkeeper"))
                (new-pool (- budget (quot budget 4))) (new-pool (quot budget 4))
                (AtomicLong. 0) (AtomicLong. -1)
                (prop-long "mnemonic.index-cache.point-cap" 512)
                (prop-long "mnemonic.index-cache.chunk-cap" 16384)
                budget
                (LongAdder.) (LongAdder.) (LongAdder.) (LongAdder.) (LongAdder.)
                (ConcurrentHashMap/newKeySet)
                (long-array (quot pbits 64)) (dec pbits)
                (java.util.concurrent.atomic.AtomicBoolean. false))))))

;; ------------------------------------------------------------------ writer side

(def ^:private ^:const n-mask (dec (bit-shift-left 1 42)))


(defn before-write!
  "Called by rocks/write! before a batch of `ops` ([:put cf k v] / [:delete cf k]) is
  applied. Stamps every touched granule with the batch's t. Returns true for an irregular
  batch (see ns doc); the caller must then call `after-write!` once the write returned or
  failed."
  [^Cache c ops]
  (let [tl (long-array 1 -1)
        idx (boolean-array 1)
        _ (doseq [op ops]
            (let [cf (nth op 1)]
              (if (identical? :log cf)
                (let [^bytes k (nth op 2)]
                  (aset tl 0 (if (== 8 (alength k))
                               (max (aget tl 0) (bit-and (raw-long k 0) n-mask))
                               Long/MAX_VALUE)))
                (when (pos? (cf-code cf)) (aset idx 0 true)))))
        t (aget tl 0)]
    (when (aget idx 0)
      ;; Regular batches stamp one at a time (the lock) with a t above every stamp so
      ;; far, so a plain ordered store (lazySet: no locked instruction, no load of the
      ;; cold slot) is the max. The full fence after the stamps orders them before the
      ;; RocksDB write, so any snapshot that contains the batch sees them.
      (locking c
       (let [^AtomicLong mt (.-max-t c)]
        (if (or (neg? t) (== t Long/MAX_VALUE) (<= t (.get mt)))
          (do (.incrementAndGet ^AtomicLong (.-epoch* c)) true)
          (let [^AtomicLongArray st (.-stripes c)
                mask (.-smask c)]
            (.set mt t)
            ;; consecutive ops mostly hit the same granules (an entity's datoms, an
            ;; attribute's chunk): a small direct-mapped filter skips repeated stamps
            (let [seen (long-array 256)]
              (doseq [op ops]
                (let [cfc (cf-code (nth op 1))]
                  (when (pos? cfc)
                    (let [^bytes k (nth op 2)
                          chunk? (or (== cfc 4) (== cfc 8))
                          n (if chunk? (if (>= (alength k) 16) 16 -1) (write-granule-len cfc k))]
                      (when (>= n 0)
                        (let [h (if chunk? (hash-chunk cfc k (key-chunk k)) (hash-range cfc k n))
                              j (bit-and h 255)]
                          (when-not (and (== h (aget seen j)) (not (zero? h)))
                            (aset seen j h)
                            (.lazySet st (unchecked-int (bit-and mask h)) t)))))))))
            (java.lang.invoke.VarHandle/fullFence)
            false)))))))

(defn after-write!
  "Ends an irregular batch (see `before-write!`)."
  [^Cache c]
  (.incrementAndGet ^AtomicLong (.-epoch* c))
  nil)

;; ------------------------------------------------------------------ reader side

(defonce ^{:doc "Set (non-nil) on the transactor thread: its reads neither probe nor fill
  the cache (what the writer reads it is usually about to change)."
           :tag ThreadLocal}
  bypass (ThreadLocal.))

(defn bypass? [] (some? (.get bypass)))

(defn epoch
  "Current epoch (even: no irregular write in progress)."
  ^long [^Cache c]
  (.get ^AtomicLong (.-epoch* c)))

(defn snapshot-taken!
  "Called right after a db's snapshot was taken: a full fence, so this thread's later
  stamp reads cannot be ordered before the (native) snapshot reads."
  []
  (java.lang.invoke.VarHandle/fullFence))

(defn stamp
  "Current stamp of the slot of hash `h`."
  ^long [^Cache c ^long h]
  (.get ^AtomicLongArray (.-stripes c) (unchecked-int (bit-and (.-smask c) h))))

(def ^:private ^ThreadLocal last-miss
  ;; [hash of this thread's last missed granule, consecutive misses of it]
  (ThreadLocal/withInitial (reify java.util.function.Supplier (get [_] (long-array 2)))))

(def ^:private ^:const run-skip
  "Consecutive misses of one granule on one thread that are taken as one read (the
  attributes of one entity read one after the other)."
  8)

(defn admit?
  "Doorkeeper: false on the first miss of hash `h` (which is remembered), true when it
  misses again within the next 1-2 windows (see Door). Up to `run-skip` consecutive
  misses of the same granule on a thread (the next attributes of the entity being read)
  count as that one miss, so a granule is admitted when a later read needs it again
  rather than on its second attribute."
  [^Cache c ^long h]
  (or (not (.-door? c))
      (let [^longs lm (.get last-miss)
            run (if (== h (aget lm 0)) (inc (aget lm 1)) 1)]
        (aset lm 0 h)
        (aset lm 1 run)
        (or (> run run-skip)
            (and (== run 1) (.seenOrAdd ^Door (.-door c) h))))))

(defn- valid-for? [^Cache c ^Seg s ^long h ^long b]
  (let [hi (.hi s)]
    (if (<= b hi)
      (<= (.lo s) b)
      (when (<= (stamp c h) hi)
        (.raiseHi s b)
        true))))

(defn lookup
  "The segment of granule `k` in `pool` that is valid for the db `rd` (an IReader), or nil."
  ^Seg [^Cache c ^Pool pool ^GKey k ^IReader rd]
  (let [^Gen g (.get ^AtomicReference (.-gen pool))
        bit (bit-and (.-pmask c) (unsigned-bit-shift-right (.-h k) 20))
        ^Seg s0 (when-not (zero? (bit-and (aget ^longs (.-present c) (unsigned-bit-shift-right bit 6))
                                          (bit-shift-left 1 (bit-and bit 63))))
                  (.get ^ConcurrentHashMap (.-m g) k))]
    (if (nil? s0)
      (do (.increment ^LongAdder (.-misses c)) nil)
      (let [h (.-h k) b (.basisT rd) ep (.epoch rd)]
        (loop [^Seg s s0]
          (cond
            (nil? s) (do (.increment ^LongAdder (.-stale c)) nil)
            (and (== ep (.-epoch s)) (valid-for? c s h b))
            (do (when-not (.refd s0) (.setRefd s0 true))
                (.increment ^LongAdder (.-hits c))
                s)
            :else (recur (.prev s))))))))

(defn- evict! [^Cache _c ^Pool pool]
  (let [^ReentrantLock lock (.-lock pool)]
    (when (.tryLock lock)
      (try
        (let [^Gen g (.get ^AtomicReference (.-gen pool))
              ^ConcurrentHashMap m (.-m g)
              ^ConcurrentLinkedQueue q (.-q g)
              ^AtomicLong w (.-weight g)
              target (- (.-budget pool) (quot (.-budget pool) 10))
              limit (+ 64 (* 2 (.size m)))]
          (loop [spins 0]
            (when (and (> (.get w) target) (< spins limit))
              (let [^Seg s (.poll q)]
                (when s
                  (cond
                    (not (zero? (.state s))) nil
                    (.refd s) (do (.setRefd s false) (.offer q s))
                    (.remove m (.-gkey s) s) (do (.setState s 2)
                                                 (.addAndGet w (- (total-weight s)))
                                                 (.increment ^LongAdder (.-evictions pool)))
                    :else nil)
                  (recur (inc spins)))))))
        (finally (.unlock lock))))))

(declare clear!)

(defn store!
  "Caches a segment of granule `k` (a key owning an exact copy of the granule bytes) read
  from the snapshot of the db `rd` (basis r, epoch), `w` being the granule's stamp read
  after that snapshot was taken. Dropped when w > r (the granule may have changed since). `ks`
  keys, `rtx` retracting txs (history) or nil; `big?` marks a granule too large to cache;
  `mut?`: some value is mutable (see mnemonic.index/mutable-keys?)."
  [^Cache c ^Pool pool ^GKey k ^IReader rd w ks rtx big? mut?]
  (let [r (.basisT rd) ep (.epoch rd) w (long w) ^bytes g (.-g k)]
  (when (and (<= w r) (>= ep 0) (== (.-len k) (alength g))
             (not (.get ^java.util.concurrent.atomic.AtomicBoolean (.-closed c))))
    (let [weight (seg-weight (.-cfc k) g ks rtx big?)]
      (when (<= weight (quot (.-budget pool) 4))
        (let [^objects ks ks ^longs rtx rtx
              n (Seg. k ks (when-not big? (object-array (alength ks))) rtx ep (boolean big?) weight
                      (boolean mut?) w r nil false 1)
              ^Gen gen (.get ^AtomicReference (.-gen pool))
              delta (long-array 1)
              top? (boolean-array 1)
              f (reify BiFunction
                  (apply [_ _ e]
                    (let [^Seg e e
                          nw (.-weight n)]
                      (cond
                        (nil? e)
                        (do (aset delta 0 nw) (aset top? 0 true) (.setState n 0) n)

                        (not= (.-epoch e) ep)
                        (if (> (.-epoch e) ep)
                          e
                          (do (aset delta 0 (- nw (total-weight e)))
                              (.setState e 2)
                              (aset top? 0 true) (.setState n 0)
                              n))

                        ;; n is at least as new: it becomes the top entry
                        (>= (.hi n) (.hi e))
                        (let [^Seg ep* (.prev e)
                              pw (if ep* (.-weight ep*) 0)]
                          (.setPrev e nil)
                          (if (>= (.hi e) (.lo n))
                            ;; overlapping ranges: same contents, n covers both
                            (do (.lowerLo n (.lo e))
                                (.setState e 2)
                                (aset delta 0 (- nw (.-weight e) pw)))
                            (do (.setState e 1)
                                (.setPrev n e)
                                (aset delta 0 (- nw pw))))
                          (aset top? 0 true) (.setState n 0)
                          n)

                        ;; n is older and overlaps e: same contents, widen e
                        (>= (.hi n) (.lo e))
                        (do (.lowerLo e (.lo n)) e)

                        ;; n is older and disjoint: keep it as e's previous version
                        :else
                        (let [^Seg ep* (.prev e)]
                          (if (or (nil? ep*) (< (.hi ep*) (.hi n)))
                            (do (.setPrev e n)
                                (aset delta 0 (- nw (if ep* (.-weight ep*) 0)))
                                e)
                            e))))))]
          (let [bit (bit-and (.-pmask c) (unsigned-bit-shift-right (.-h k) 20))
                ^longs pr (.-present c)
                w (unsigned-bit-shift-right bit 6)]
            (aset pr w (bit-or (aget pr w) (bit-shift-left 1 (bit-and bit 63)))))
          ;; n's state is set inside the compute (before n is published as the top
          ;; entry), never after: a concurrent replacement may already have retired it
          (.compute ^ConcurrentHashMap (.-m gen) k f)
          (when (aget top? 0)
            (.offer ^ConcurrentLinkedQueue (.-q gen) n))
          ;; a fill racing close!: drop what it may have put back
          (when (.get ^java.util.concurrent.atomic.AtomicBoolean (.-closed c))
            (clear! c))
          (.increment (if big? ^LongAdder (.-bigs c) ^LongAdder (.-fills c)))
          (when (> (.addAndGet ^AtomicLong (.-weight gen) (aget delta 0)) (.-budget pool))
            (evict! c pool))
          n))))))

(defn clear!
  "Drops every segment and the admission history (stamps are kept: they stay correct)."
  [^Cache c]
  (when c
    (doseq [^Pool p [(.-point c) (.-scan c)]]
      (.set ^AtomicReference (.-gen p) (new-gen)))
    (.clear ^java.util.Set (.-big-scans c))
    (Arrays/fill ^longs (.-present c) 0)
    (.reset ^Door (.-door c)))
  nil)

(defn close!
  "The store closed: drops every segment and refuses later fills."
  [^Cache c]
  (when c
    (.set ^java.util.concurrent.atomic.AtomicBoolean (.-closed c) true)
    (clear! c))
  nil)

(defn stats
  "Counters and sizes: {:hits :misses :stale :fills :big :evictions :entries :bytes
  :point-bytes :scan-bytes :budget-bytes}. Misses count probes that found no entry,
  stale those whose entry was not valid for the reader's basis."
  [^Cache c]
  (when c
    (let [gp ^Gen (.get ^AtomicReference (.-gen ^Pool (.-point c)))
          gs ^Gen (.get ^AtomicReference (.-gen ^Pool (.-scan c)))
          pb (.get ^AtomicLong (.-weight gp))
          sb (.get ^AtomicLong (.-weight gs))]
      {:hits (.sum ^LongAdder (.-hits c))
       :misses (.sum ^LongAdder (.-misses c))
       :stale (.sum ^LongAdder (.-stale c))
       :fills (.sum ^LongAdder (.-fills c))
       :big (.sum ^LongAdder (.-bigs c))
       :evictions (+ (.sum ^LongAdder (.-evictions ^Pool (.-point c)))
                     (.sum ^LongAdder (.-evictions ^Pool (.-scan c))))
       :entries (+ (.size ^ConcurrentHashMap (.-m gp)) (.size ^ConcurrentHashMap (.-m gs)))
       :bytes (+ pb sb)
       :point-bytes pb
       :scan-bytes sb
       :budget-bytes (.-budget c)})))

(defn reset-stats!
  "Zeroes the hit/miss/fill counters (benchmarks)."
  [^Cache c]
  (when c
    (doseq [^LongAdder a [(.-hits c) (.-misses c) (.-stale c) (.-fills c) (.-bigs c)
                          (.-evictions ^Pool (.-point c)) (.-evictions ^Pool (.-scan c))]]
      (.reset a)))
  nil)
