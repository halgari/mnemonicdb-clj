(ns mnemonic.index
  "Cursor and db protocols plus cursor combinators over encoded index keys."
  (:require [mnemonic.codec :as codec]
            [mnemonic.datom :as datom]
            [mnemonic.rocks :as rocks])
  (:import (clojure.lang ChunkBuffer ChunkedCons IReduceInit Reduced Seqable Sequential)
           (java.lang.ref Reference)
           (java.util Arrays)
           (java.util.concurrent.atomic AtomicLong)
           (mnemonic.datom Datom IDatomView)
           (mnemonic.rocks IRawCursor)))

(set! *warn-on-reflection* true)

(definterface ICursor
  (^boolean valid [])
  (advance [])
  (^bytes key [])
  (datom [])
  (^long retractTx [])
  (close []))

(definterface IKeyView
  (^bytes keyBuf [] "Buffer holding the current key in its first keyLen bytes; valid until advance. Never retain.")
  (^long keyLen []))

(definterface IViewCursor
  (view [] "Flyweight IDatomView of the current entry, valid until the cursor advances."))

(defn view-of
  "Flyweight view of `c`'s current entry (a materialized Datom for cursors that cannot
  decode in place). Valid until `c` advances; never retain."
  ^IDatomView [^ICursor c]
  (if (instance? IViewCursor c) (.view ^IViewCursor c) (.datom c)))

(defn key-buf
  "The current key of `c` without copying (see IKeyView); only valid until `c` advances."
  ^bytes [^ICursor c]
  (if (instance? IKeyView c) (.keyBuf ^IKeyView c) (.key c)))

(defn key-len
  "Length of the current key in `(key-buf c)`."
  ^long [^ICursor c]
  (if (instance? IKeyView c) (.keyLen ^IKeyView c) (alength ^bytes (.key c))))

(defn compare-keys
  "Unsigned lexicographic comparison of the current keys of two valid cursors."
  ^long [^ICursor a ^ICursor b]
  (long (Arrays/compareUnsigned (key-buf a) 0 (unchecked-int (key-len a))
                                (key-buf b) 0 (unchecked-int (key-len b)))))

(defprotocol IDb
  (-seek-current [db index prefix-bytes]
    "Raw current entries of `db`'s base (layers such as TimeView pass this through
    without applying their bounds; use -view for reads).")
  (-seek-history [db index prefix-bytes]
    "Raw history entries (retractTx = retracting tx) of `db`'s base; like
    -seek-current, time views pass it through unbounded.")
  (-view [db index prefix-bytes])
  (-seek-view [db index start-bytes])
  (-basis-t [db])
  (-schema [db])
  (-as-of-t [db])
  (-since-t [db])
  (-history? [db])
  (-filter-pred [db])
  (-raw [db]))

(defonce ^{:doc "Test hook: number of range cursors opened and not yet closed."
           :tag AtomicLong}
  open-cursors (AtomicLong. 0))

;; ------------------------------------------------------------------ range cursor

(defn- in-prefix-len
  "`n` (a key length in `kb`, -1 = exhausted) when the key is within `prefix` (or
  `prefix` is nil), else -1."
  ^long [^long n ^bytes kb ^bytes prefix]
  (if (and (>= n 0)
           (or (nil? prefix)
               (let [pl (alength prefix)]
                 (and (>= n pl) (Arrays/equals kb 0 pl prefix 0 pl)))))
    n
    -1))

(defn- stale-view []
  (throw (IllegalStateException. "Datom view read after its cursor was exhausted or closed")))

;; The current key lives in the raw cursor's reused buffer (`klen` bytes, -1 = invalid).
;; `key` materializes an exact-length copy lazily (callers may retain it); `datom` decodes
;; straight from the buffer.
(deftype RangeCursor [^IRawCursor raw ^bytes prefix index history? ^long code
                      ^:unsynchronized-mutable ^long klen
                      ^:unsynchronized-mutable ^bytes k
                      ^:unsynchronized-mutable d
                      ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (>= klen 0))
  (advance [_]
    (when (>= klen 0)
      (set! klen (in-prefix-len (.nextInto raw) (.kbuf raw) prefix))
      (set! k nil)
      (set! d nil))
    nil)
  (key [_]
    (when (>= klen 0)
      (or k (set! k (Arrays/copyOf (.kbuf raw) (unchecked-int klen))))))
  IKeyView
  (keyBuf [_] (when (>= klen 0) (or k (.kbuf raw))))
  (keyLen [_] klen)
  (datom [_]
    (when (>= klen 0)
      (or d (set! d (codec/decode-key-code code (.kbuf raw) klen)))))
  (retractTx [_]
    (if (and (>= klen 0) history?)
      (.valueLong raw)
      -1))
  IViewCursor
  (view [this] (when (>= klen 0) this))
  ;; The cursor is its own flyweight view: components decode in place from the raw
  ;; cursor's key buffer (valid until advance).
  IDatomView
  ;; a view read after the cursor moved past its end or closed would decode a recycled
  ;; buffer: fail loudly instead
  (eid [_] (when (neg? klen) (stale-view)) (codec/key-e code (.kbuf raw) klen))
  (attr [_] (when (neg? klen) (stale-view)) (codec/key-a code (.kbuf raw) klen))
  (value [_] (when (neg? klen) (stale-view)) (codec/key-value code (.kbuf raw) klen))
  (valueLong [_] (when (neg? klen) (stale-view)) (codec/key-value-long code (.kbuf raw) klen))
  (txid [_] (when (neg? klen) (stale-view)) (codec/key-tx (.kbuf raw) klen))
  (isAdded [_] (when (neg? klen) (stale-view)) true)
  (toDatom [this] (when (neg? klen) (stale-view)) (.datom this))
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (set! klen -1)
      (set! k nil)
      (set! d nil)
      (.decrementAndGet open-cursors)
      (.close raw))
    nil))

(defn- range-cursor [^IRawCursor raw ^bytes start ^bytes prefix index history?]
  (.incrementAndGet open-cursors)
  (try
    (->RangeCursor raw prefix index history? (codec/index-code index)
                   (in-prefix-len (.seekInto raw start) (.kbuf raw) prefix) nil nil false)
    (catch Throwable t
      (.decrementAndGet open-cursors)
      (.close raw)
      (throw t))))

(defn prefix-cursor
  "Cursor over keys of `raw-cursor` starting with `prefix`; owns and closes the raw cursor."
  [raw-cursor ^bytes prefix index history?]
  (range-cursor raw-cursor prefix prefix index history?))

(defn seek-cursor
  "Cursor from the first key >= `start` to the end of the index; owns the raw cursor."
  [raw-cursor ^bytes start index history?]
  (range-cursor raw-cursor start nil index history?))

;; ------------------------------------------------------------------ segment cursor

(defn- mutable-value? [v]
  (or (instance? java.util.Date v) (bytes? v)))

(defn- copy-value [v]
  (cond
    (instance? java.util.Date v) (java.util.Date. (.getTime ^java.util.Date v))
    (bytes? v) (aclone ^bytes v)
    :else v))

(defn mutable-keys?
  "True when some key of `ks` (index code `code`) holds a value that decodes to a mutable
  object (instant 0x16, bytes 0x1c, or a tuple 0x1d that may hold one): its segment's
  Datoms must be detached on the way out. Computed once per segment, at fill."
  [^long code ^objects ks]
  (and (not (== code 3))
       (let [off (if (== code 2) 8 16)]
         (loop [i 0]
           (if (< i (alength ks))
             (let [^bytes k (aget ks i)]
               (if (and (> (alength k) off)
                        (let [t (bit-and 0xff (long (aget k off)))]
                          (or (== t 0x16) (== t 0x1c) (== t 0x1d))))
                 true
                 (recur (inc i))))
             false)))))

(defn detach
  "`d` itself, or a copy whose mutable value (java.util.Date, byte[], or a tuple holding
  one) is a fresh copy: the cached Datom is shared by every reader, and a caller that
  mutates the value it got must not change what later reads see (an uncached read
  decodes a fresh value every time)."
  ^Datom [^Datom d]
  (let [v (.-v d)]
    (cond
      (mutable-value? v)
      (Datom. (.-e d) (.-a d) (copy-value v) (.-tx d) (.-added d))
      (and (vector? v) (some mutable-value? v))
      (Datom. (.-e d) (.-a d) (mapv copy-value v) (.-tx d) (.-added d))
      :else d)))

(defn seg-datom
  "The Datom of entry `i` of a cached segment (mnemonic.segcache): decoded on first
  access and stored back into the shared array `ds`. A racy but benign write: decoding is
  deterministic and a Datom's fields are final, so another thread sees nil (and decodes
  again) or a complete Datom. Callers detach the Datoms of segments holding mutable
  values (`mutable-keys?`, `detach`)."
  ^Datom [^long code ^objects ks ^objects ds ^long i]
  (let [d (aget ds i)]
    (if (nil? d)
      (let [^bytes k (aget ks i)
            d (codec/decode-key-code code k (alength k))]
        (aset ds i d)
        d)
      d)))

;; Entries [i, end) of a cached segment: shared, immutable key arrays (`ks`), lazily
;; decoded Datoms (`ds`) and, for history CFs, retracting txs (`rtx`). No native call and
;; no allocation per entry once decoded; the Datom is the flyweight view.
(deftype SegCursor [^long code ^objects ks ^objects ds ^longs rtx ^boolean mut
                    ^:unsynchronized-mutable ^long i
                    ^:unsynchronized-mutable ^long end]
  ICursor
  (valid [_] (< i end))
  (advance [_] (when (< i end) (set! i (inc i))) nil)
  (key [_] (when (< i end) (aget ks i)))
  (datom [_] (when (< i end) (let [d (seg-datom code ks ds i)] (if mut (detach d) d))))
  (retractTx [_] (if (and rtx (< i end)) (aget rtx i) -1))
  (close [_] (set! end i) nil)
  IKeyView
  (keyBuf [_] (when (< i end) (aget ks i)))
  (keyLen [_] (if (< i end) (alength ^bytes (aget ks i)) -1))
  IViewCursor
  (view [_] (when (< i end) (let [d (seg-datom code ks ds i)] (if mut (detach d) d)))))

(defn- starts-with? [^bytes k ^bytes prefix]
  (let [n (alength prefix)]
    (and (>= (alength k) n) (Arrays/equals k 0 n prefix 0 n))))

(defn seg-lower-bound
  "Index of the first key of `ks` >= `x` (unsigned order)."
  ^long [^objects ks ^bytes x]
  (loop [lo 0 hi (alength ks)]
    (if (< lo hi)
      (let [mid (unsigned-bit-shift-right (+ lo hi) 1)]
        (if (neg? (Arrays/compareUnsigned ^bytes (aget ks mid) x))
          (recur (inc mid) hi)
          (recur lo mid)))
      lo)))

(defn seg-cursor
  "Cursor over the entries of segment arrays `ks`/`ds`/`rtx` (index `index`) that start
  with `prefix` (every entry when `whole?`); `mut?`: the segment holds mutable values."
  [index ^objects ks ^objects ds ^longs rtx mut? ^bytes prefix whole?]
  (let [code (codec/index-code index)
        n (alength ks)]
    (if whole?
      (SegCursor. code ks ds rtx (boolean mut?) 0 n)
      (let [lo (seg-lower-bound ks prefix)
            end (loop [j lo]
                  (if (and (< j n) (starts-with? (aget ks j) prefix)) (recur (inc j)) j))]
        (SegCursor. code ks ds rtx (boolean mut?) lo end)))))

;; ------------------------------------------------------------------ combinators

(deftype EmptyCursor []
  ICursor
  (valid [_] false)
  (advance [_] nil)
  (key [_] nil)
  (datom [_] nil)
  (retractTx [_] -1)
  (close [_] nil))

(defn empty-cursor [] (EmptyCursor.))

(defn- min-index
  "Index of the valid cursor with the smallest key (first on ties), or -1."
  ^long [^objects cs]
  (loop [i 0 best -1]
    (if (< i (alength cs))
      (let [^mnemonic.index.ICursor c (aget cs i)]
        (if (.valid c)
          (recur (inc i)
                 (if (or (neg? best)
                         (neg? (compare-keys c (aget cs best))))
                   i
                   best))
          (recur (inc i) best)))
      best)))

(deftype MergeCursor [^objects cs
                      ^:unsynchronized-mutable ^long cur
                      ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (>= cur 0))
  (advance [_]
    (when (>= cur 0)
      (.advance ^mnemonic.index.ICursor (aget cs cur))
      (set! cur (min-index cs)))
    nil)
  (key [_] (when (>= cur 0) (.key ^mnemonic.index.ICursor (aget cs cur))))
  IKeyView
  (keyBuf [_] (when (>= cur 0) (key-buf (aget cs cur))))
  (keyLen [_] (if (>= cur 0) (key-len (aget cs cur)) -1))
  (datom [_] (when (>= cur 0) (.datom ^mnemonic.index.ICursor (aget cs cur))))
  (retractTx [_] (if (>= cur 0) (.retractTx ^mnemonic.index.ICursor (aget cs cur)) -1))
  IViewCursor
  (view [_] (when (>= cur 0) (view-of (aget cs cur))))
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (set! cur -1)
      (let [errs (volatile! nil)]
        (dotimes [i (alength cs)]
          (try (.close ^mnemonic.index.ICursor (aget cs i))
               (catch Throwable t (vswap! errs #(or % t)))))
        (when-let [t @errs] (throw t))))
    nil))

(defn merge-cursors
  "K-way merge of cursors by key bytes (ties resolved in argument order).
  Closing the merge closes every source."
  [cursors]
  (let [cs (object-array cursors)]
    (->MergeCursor cs (min-index cs) false)))

(deftype FilterCursor [^mnemonic.index.ICursor c pred
                       ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (and (not closed?) (.valid c)))
  (advance [_]
    (when-not closed?
      (.advance c)
      (loop []
        (when (and (.valid c) (not (pred (.datom c))))
          (.advance c)
          (recur))))
    nil)
  (key [_] (when-not closed? (.key c)))
  (datom [_] (when-not closed? (.datom c)))
  (retractTx [_] (if closed? -1 (.retractTx c)))
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (.close c))
    nil)
  ;; `pred` (user code) is always handed a Datom; consumers of the kept entries get views
  IViewCursor
  (view [_] (view-of c))
  IKeyView
  (keyBuf [_] (key-buf c))
  (keyLen [_] (key-len c)))

(defn filter-cursor
  "Cursor over `c` keeping only entries whose Datom satisfies `pred`."
  [^mnemonic.index.ICursor c pred]
  (try
    (loop []
      (when (and (.valid c) (not (pred (.datom c))))
        (.advance c)
        (recur)))
    (catch Throwable t
      (.close c)
      (throw t)))
  (->FilterCursor c pred false))

;; ------------------------------------------------------------------ iterable

(def ^:private ^:const chunk-size 512)

(defn copy-datom
  "Datoms produced by cursors are immutable (`codec/decode-key` allocates a fresh Datom
  per key), so no copy is needed: returns `d`. Kept for callers outside this namespace.
  If a cursor ever hands out a reused/mutable Datom, copying must come back here."
  ^Datom [^Datom d]
  d)

(defn- cursor-seq
  "Lazy chunked seq over `cur`, closing it at exhaustion. `holder` is kept
  reachable by the seq so the Cleaner closes `cur` if the seq is abandoned; `db` is
  kept reachable so its snapshot cannot be released under a live iteration."
  [^mnemonic.index.ICursor cur holder db]
  (lazy-seq
   (try
     (let [buf (ChunkBuffer. chunk-size)]
       (loop [n 0]
         (when (and (< n chunk-size) (.valid cur))
           (.add buf (.datom cur))
           (.advance cur)
           (recur (inc n))))
       (let [more? (.valid cur)]
         (when-not more? (.close cur))
         (when (pos? (.count buf))
           (ChunkedCons. (.chunk buf)
                         (when more? (cursor-seq cur holder db))))))
     (catch Throwable t
       (.close cur)
       (throw t))
     (finally
       (Reference/reachabilityFence db)))))

(definterface IViewReduce
  (reduceViews [f init] "Like reduce, but `f` receives flyweight IDatomViews (valid only during the call)."))

(def miss
  "What `IFastReduce/fastReduce` returns when it cannot serve the read."
  (Object.))

(definterface IFastReduce
  (fastReduce [index ^bytes prefix f init]
    "Like reduce-views over (-view db index prefix), straight from a cached segment (no
    cursor); `miss` when that isn't possible, for the caller to read the usual way.")
  (scanEntities [^long lo ^long hi ^java.util.Set ids f]
    "Walks the EAVT entries of entities `lo`..`hi` in one range scan, calling (f view) for
    those of the entities in `ids` (Longs) and caching the segments of those entities
    as the cache's own reads would; `miss` when the db has no cache to fill."))

(defn reduce-views
  "Reduce `coll` (a `datoms-iterable`, or any reducible of Datoms) with `f`, which receives
  flyweight IDatomViews decoded in place from the cursor buffer: no Datom, key copy or
  value is allocated unless `f` asks. A view is valid only during the call to `f`; call
  `.toDatom` to keep one. Used by internal consumers; public seqs and `reduce` copy."
  [coll f init]
  (if (instance? IViewReduce coll)
    (.reduceViews ^IViewReduce coll f init)
    (reduce f init coll)))

(defn datoms-iterable
  "Iterable of immutable Datoms over `(-view db index prefix)`. Each seq / iterator /
  reduce opens its own cursor."
  [db index prefix]
  (reify
    Iterable
    (iterator [this]
      (let [^Iterable s (or (seq this) [])] (.iterator s)))
    Seqable
    (seq [_]
      (let [^mnemonic.index.ICursor cur (-view db index prefix)
            holder (Object.)]
        (rocks/register-cleanup! holder #(.close cur))
        (seq (cursor-seq cur holder db))))
    IViewReduce
    (reduceViews [_ f init]
      (let [r (if (instance? IFastReduce db)
                (try (.fastReduce ^IFastReduce db index prefix f init)
                     (finally (Reference/reachabilityFence db)))
                miss)]
        (if-not (identical? r miss)
          r
          (let [^mnemonic.index.ICursor cur (-view db index prefix)]
            (try
              (loop [acc init]
                (if (.valid cur)
                  (let [ret (f acc (view-of cur))]
                    (if (reduced? ret)
                      (.deref ^Reduced ret)
                      (do (.advance cur) (recur ret))))
                  acc))
              (finally
                (.close cur)
                (Reference/reachabilityFence db)))))))
    IReduceInit
    (reduce [_ f init]
      (let [^mnemonic.index.ICursor cur (-view db index prefix)]
        (try
          (loop [acc init]
            (if (.valid cur)
              (let [ret (f acc (.datom cur))]
                (if (reduced? ret)
                  (.deref ^Reduced ret)
                  (do (.advance cur) (recur ret))))
              acc))
          (finally
            (.close cur)
            (Reference/reachabilityFence db)))))
    Sequential))
