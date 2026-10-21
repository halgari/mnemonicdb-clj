(ns mnemonic.db
  "Base db: a RocksDB snapshot plus basis-t and schema."
  (:require [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.index :as index]
            [mnemonic.rocks :as rocks]
            [mnemonic.schema :as schema]
            [mnemonic.segcache :as sc])
  (:import (java.util ArrayList Arrays)
           (mnemonic.datom Datom)
           (mnemonic.index ICursor)
           (mnemonic.rocks IRawCursor Snap Store)
           (mnemonic.segcache Cache GKey IReader Seg)))

(set! *warn-on-reflection* true)

(def ^:private history-cf
  {:eavt :eavt-h :aevt :aevt-h :avet :avet-h :vaet :vaet-h})

;; Snapshot release action. Must not reference the Base (it is the Cleaner action).
(deftype Releaser [^Snap snap]
  Runnable
  (run [_] (.markReleased snap)))

(defn- open-raw
  "A raw cursor over `cf`. `prefix` non-nil: the caller reads only keys starting with it
  (a prefix cursor), which lets RocksDB use the prefix bloom filter; nil: total order."
  ^IRawCursor [store snap cf prefix]
  (rocks/raw-cursor store cf snap prefix))

;; ------------------------------------------------------------------ segment cache reads
;; Base's raw sources go through the store's heap segment cache (mnemonic.segcache), so
;; every layer above (index, layers, entity, pull, query, api) gets it unchanged. A miss
;; reads RocksDB through the normal guarded cursors; a hit serves heap arrays but first
;; throws exactly as opening a native cursor would on a closed store or released db.

(defn- check-open! [store ^Snap snap]
  (when (rocks/closed? store) (throw (rocks/connection-released-ex)))
  (when (rocks/snapshot-released? snap) (throw (rocks/db-released-ex))))

(defn- plain-cursor [store snap cf idx hist? ^bytes prefix]
  (index/prefix-cursor (open-raw store snap cf prefix) prefix idx hist?))

(defn- hit-cursor [store snap idx ^Seg s ^bytes prefix whole?]
  (check-open! store snap)
  (index/seg-cursor idx (sc/seg-keys s) (sc/seg-datoms s) (sc/seg-rtx s) prefix whole?))

(defn- long-array-of ^longs [^ArrayList xs]
  (let [n (.size xs) a (long-array n)]
    (dotimes [i n] (aset a i (long (.get xs i))))
    a))

(defn- fill-point
  "Miss path of a point granule (the first (.-len probe) bytes of `prefix`): reads the
  whole granule (at most point-cap entries), caches it and serves `prefix` from it; a
  larger granule is marked big and `prefix` is read from RocksDB."
  [store snap ^Cache c ^IReader rd cf idx hist? ^bytes prefix ^GKey probe]
  (let [n (alength prefix)
        g (Arrays/copyOf prefix (unchecked-int (.-len probe)))
        k (sc/gkey (.-cfc probe) g (alength g))
        w (sc/stamp c (sc/gkey-hash k))
        cap (sc/point-cap c)
        ^ICursor cur (index/prefix-cursor (open-raw store snap cf g) g idx hist?)
        kl (ArrayList.)
        rl (when hist? (ArrayList.))
        big? (try
               (loop [i 0]
                 (cond
                   (not (.valid cur)) false
                   (>= i cap) true
                   :else (do (.add kl (.key cur))
                             (when rl (.add rl (.retractTx cur)))
                             (.advance cur)
                             (recur (inc i)))))
               (finally (.close cur)))]
    (if big?
      (do (sc/store! c (sc/point-pool c) k rd w nil nil true)
          (plain-cursor store snap cf idx hist? prefix))
      (let [ka (.toArray kl)
            ra (when rl (long-array-of rl))
            s (sc/store! c (sc/point-pool c) k rd w ka ra false)]
        (index/seg-cursor idx ka (if s (sc/seg-datoms s) (object-array (alength ka))) ra
                          prefix (== n (alength g)))))))

(defn- point-cursor [store snap ^Cache c ^IReader rd cf idx hist? ^bytes prefix cfc n]
  (let [k (sc/gkey cfc prefix n)
        s (sc/lookup c (sc/point-pool c) k rd)]
    (cond
      (nil? s) (if (sc/admit? c (sc/gkey-hash k))
                 (fill-point store snap c rd cf idx hist? prefix k)
                 (plain-cursor store snap cf idx hist? prefix))
      (sc/seg-big? s) (plain-cursor store snap cf idx hist? prefix)
      :else (hit-cursor store snap idx s prefix (== (long n) (alength prefix))))))

;; AEVT scans walk an attribute chunk by chunk ([a, e >>> 10]): a cached chunk is served
;; from the heap, a missing one is read from RocksDB (a raw cursor of this snapshot) and
;; cached. After a complete chunk the next chunk is tried in the cache first; only a gap
;; (or a miss) costs a seek. A chunk above chunk-cap entries is marked big and streamed.

(defn- in-attr? [^IRawCursor raw ^long klen ^bytes prefix]
  (and (>= klen 16) (Arrays/equals (.kbuf raw) 0 8 prefix 0 8)))

(defn- in-chunk? [^IRawCursor raw ^long klen ^bytes prefix ^long ch]
  (and (in-attr? raw klen prefix) (== ch (sc/key-chunk (.kbuf raw)))))

(definterface IChunks
  (batch [^"[Ljava.lang.Object;" ks ^"[Ljava.lang.Object;" ds ^"[J" rtx ^long chunk ^long mode])
  (readRaw [^long klen ^long chunk k])
  (openAt [^long klen ^long kch known])
  (settle []))

;; mode: 0 done, 1 the batch is a complete chunk, 2 the batch is part of a big chunk (the
;; raw cursor holds the next unread key, rlen bytes)
(deftype ChunkCursor [store ^Snap snap ^Cache c ^IReader rd ^long cfc ^boolean hist?
                      ^bytes prefix ^IRawCursor raw ^long cap ^long ah
                      ^:unsynchronized-mutable ^long filled
                      ^:unsynchronized-mutable ^boolean fill?
                      ^:unsynchronized-mutable ^objects ks
                      ^:unsynchronized-mutable ^objects ds
                      ^:unsynchronized-mutable ^longs rtx
                      ^:unsynchronized-mutable ^long i
                      ^:unsynchronized-mutable ^long end
                      ^:unsynchronized-mutable ^long chunk
                      ^:unsynchronized-mutable ^long mode
                      ^:unsynchronized-mutable ^long rlen
                      ^:unsynchronized-mutable ^boolean closed?]
  IChunks
  (batch [_ ks* ds* rtx* ch m]
    (set! ks ks*) (set! ds ds*) (set! rtx rtx*)
    (set! i 0) (set! end (long (alength ks*)))
    (set! chunk ch) (set! mode m)
    nil)
  (readRaw [this klen ch k]
    ;; the raw cursor holds the first unread key of chunk `ch`; `k` non-nil: cache it
    (let [^GKey k k
          w (when k (sc/stamp c (sc/gkey-hash k)))
          kl (ArrayList.)
          rl (when hist? (ArrayList.))
          nxt (loop [klen klen n 0]
                (if (and (< n cap) (in-chunk? raw klen prefix ch))
                  (do (.add kl (Arrays/copyOf (.kbuf raw) (unchecked-int klen)))
                      (when rl (.add rl (.valueLong raw)))
                      (recur (.nextInto raw) (inc n)))
                  klen))
          complete? (not (in-chunk? raw nxt prefix ch))
          ka (.toArray kl)
          ra (when rl (long-array-of rl))
          ^Seg s (when k
                   (if complete?
                     (sc/store! c (sc/scan-pool c) k rd w ka ra false)
                     (sc/store! c (sc/scan-pool c) k rd w nil nil true)))]
      (when s
        (set! filled (+ filled (sc/seg-weight-of s)))
        ;; an attribute larger than the scan budget would only churn the scan pool
        (when (> filled (quot (sc/scan-budget c) 2))
          (sc/mark-big-scan! c ah)
          (set! fill? (boolean false))))
      (set! rlen (long nxt))
      (.batch this ka (if (and s complete?) (sc/seg-datoms s) (object-array (alength ka))) ra
              ch (if complete? 1 2))))
  (openAt [this klen kch known]
    ;; the raw cursor holds the next key to serve (klen bytes, -1 none); `known` is the
    ;; lookup result for chunk `kch` when that chunk was already probed
    (if-not (in-attr? raw klen prefix)
      (do (set! mode 0) (set! i 0) (set! end 0) nil)
      (let [ch (sc/key-chunk (.kbuf raw))
            k (sc/gkey cfc (sc/chunk-granule prefix ch) 16)
            ^Seg s (if (== ch kch) known (sc/lookup c (sc/scan-pool c) k rd))]
        (cond
          (nil? s) (.readRaw this klen ch (when fill? k))
          (sc/seg-big? s) (.readRaw this klen ch nil)
          :else (.batch this (sc/seg-keys s) (sc/seg-datoms s) (sc/seg-rtx s) ch 1)))))
  (settle [this]
    (loop []
      (when (and (>= i end) (pos? mode))
        (if (== mode 1)
          (let [nc (inc chunk)]
            (if (> nc sc/max-chunk)
              (do (set! mode 0) nil)
              (let [k (sc/gkey cfc (sc/chunk-granule prefix nc) 16)
                    ^Seg s (sc/lookup c (sc/scan-pool c) k rd)]
                (if (and s (not (sc/seg-big? s)) (pos? (sc/seg-size s)))
                  (.batch this (sc/seg-keys s) (sc/seg-datoms s) (sc/seg-rtx s) nc 1)
                  (.openAt this (.seekInto raw (sc/chunk-start prefix nc)) nc s)))))
          (if (in-chunk? raw rlen prefix chunk)
            (.readRaw this rlen chunk nil)
            (.openAt this rlen -1 nil)))
        (recur)))
    nil)
  ICursor
  (valid [_] (< i end))
  (advance [this]
    (when (< i end)
      (set! i (inc i))
      (when (>= i end) (.settle this)))
    nil)
  (key [_] (when (< i end) (aget ks i)))
  (datom [_] (when (< i end) (index/seg-datom 1 ks ds i)))
  (retractTx [_] (if (and rtx (< i end)) (aget rtx i) -1))
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (set! mode 0) (set! i 0) (set! end 0)
      (.close raw))
    nil)
  mnemonic.index.IKeyView
  (keyBuf [_] (when (< i end) (aget ks i)))
  (keyLen [_] (if (< i end) (alength ^bytes (aget ks i)) -1))
  mnemonic.index.IViewCursor
  (view [_] (when (< i end) (index/seg-datom 1 ks ds i))))

(defn- chunk-cursor [store snap ^Cache c rd cf cfc hist? ^bytes prefix ah]
  (let [^IRawCursor raw (open-raw store snap cf nil)]
    (try
      (let [cur (ChunkCursor. store snap c rd cfc hist? prefix raw (sc/chunk-cap c) ah 0 true
                              (object-array 0) nil nil 0 0 -1 0 -1 false)]
        (.openAt cur (.seekInto raw prefix) -1 nil)
        (.settle cur)
        cur)
      (catch Throwable t (.close raw) (throw t)))))

(defn- chunk-probe
  "[a e ...] on AEVT: served from a cached chunk when there is one, never filled (chunks
  are filled by scans)."
  [store snap ^Cache c rd cf idx hist? ^bytes prefix cfc]
  (let [s (sc/lookup c (sc/scan-pool c)
                     (sc/gkey cfc (sc/chunk-granule prefix (sc/key-chunk prefix)) 16) rd)]
    (if (and s (not (sc/seg-big? s)))
      (hit-cursor store snap idx s prefix false)
      (plain-cursor store snap cf idx hist? prefix))))

(defn- cached-cursor
  "Cursor over the entries of `cf` (`idx`, or its history CF when `hist?`) under `prefix`
  for the db `rd`, through the segment cache when it can serve that prefix shape."
  [store ^Snap snap ^IReader rd cf idx hist? ^bytes prefix]
  (let [^Cache c (.-segcache ^Store store)
        len (alength prefix)]
    (if (or (nil? c) (neg? (.epoch rd)) (sc/bypass?))
      (plain-cursor store snap cf idx hist? prefix)
      (let [cfc (sc/cf-code cf)]
        (case cfc
          (1 2 5 6) (if (>= len 8)
                      (point-cursor store snap c rd cf idx hist? prefix cfc 8)
                      (plain-cursor store snap cf idx hist? prefix))
          (3 7) (let [e (sc/value-end prefix 8 len)
                      r (- len e)]
                  (if (and (pos? e) (or (== r 0) (== r 8) (== r 16)))
                    (point-cursor store snap c rd cf idx hist? prefix cfc e)
                    (plain-cursor store snap cf idx hist? prefix)))
          (4 8) (cond
                  (== len 8) (let [ah (sc/gkey-hash (sc/gkey cfc prefix 8))]
                               (if (and (not (sc/big-scan? c ah)) (sc/admit? c ah))
                                 (chunk-cursor store snap c rd cf cfc hist? prefix ah)
                                 (plain-cursor store snap cf idx hist? prefix)))
                  (>= len 16) (chunk-probe store snap c rd cf idx hist? prefix cfc)
                  :else (plain-cursor store snap cf idx hist? prefix))
          (plain-cursor store snap cf idx hist? prefix))))))

;; ep: the segment cache epoch at snapshot time (-1: this db never uses the cache)
(deftype Base [store ^Snap snap basis-t schema meta ^Releaser releaser ^long ep]
  IReader
  (basisT [_] (long basis-t))
  (epoch [_] ep)
  index/IDb
  (-seek-current [this idx prefix]
    (cached-cursor store snap this idx idx false prefix))
  (-seek-history [this idx prefix]
    (cached-cursor store snap this (history-cf idx) idx true prefix))
  (-view [this idx prefix]
    (cached-cursor store snap this idx idx false prefix))
  (-seek-view [_ idx start]
    (index/seek-cursor (open-raw store snap idx nil) start idx false))
  (-basis-t [_] basis-t)
  (-schema [_] schema)
  (-as-of-t [_] nil)
  (-since-t [_] nil)
  (-history? [_] false)
  (-filter-pred [_] nil)
  (-raw [this] this))

(defn history-seek-cursor
  "Cursor over the history index of `idx` from the first key >= `start` to its end."
  [^Base b idx start]
  (index/seek-cursor (open-raw (.-store b) (.-snap b) (history-cf idx) nil) start idx true))

(defn released?
  "True once `release-db` was called on `db` (or it was collected)."
  [^Base db]
  (rocks/snapshot-released? (.-snap db)))

(defn retire-pool!
  "Closes the idle pooled native iterators of `db` without releasing its snapshot. For a
  superseded published db: `db` and every cursor on it stay valid (later reads open fresh
  iterators), but it no longer pins memtables/SSTs through idle iterators until the GC
  runs its Cleaner. Writer thread, after publishing the successor."
  [^Base db]
  (rocks/retire-snapshot! (.-store db) (.-snap db))
  nil)

(defn db-meta
  "The store meta (counters) as of this db's snapshot."
  [^Base db]
  (.-meta db))

(defn release-db
  "Release the snapshot held by `db` (idempotent). Later reads of `db` throw
  :db.error/db-released; iterations already in progress complete."
  [^Base db]
  (.run ^Runnable (.-releaser db))
  nil)

(defn- last-log-tx
  "Highest tx in the log as seen by `snap`, or nil."
  [store snap]
  (with-open [^IRawCursor c (rocks/raw-cursor store :log snap)]
    (.seekToLast c)
    (when (.isValid c)
      (codec/bytes->long (.key c)))))

(defn- scan-attr
  "All current datoms of attribute `aid`, via AEVT on `snap`."
  [store snap aid]
  (let [^mnemonic.index.ICursor cur (index/prefix-cursor (rocks/raw-cursor store :aevt snap)
                                 (codec/encode-prefix :aevt [aid]) :aevt false)]
    (try
      (loop [acc []]
        (if (.valid cur)
          (let [d (index/copy-datom (.datom cur))]
            (.advance cur)
            (recur (conj acc d)))
          acc))
      (finally (.close cur)))))

(defn ident-history
  "[eid keyword retract-tx] of every retracted :db/ident datom, read from `cur` (a
  history cursor over the AEVT prefix of :db/ident); closes `cur`."
  [^mnemonic.index.ICursor cur]
  (try
    (loop [acc []]
      (if (.valid cur)
        (let [^Datom d (.datom cur)
              x [(.-e d) (.-v d) (.retractTx cur)]]
          (.advance cur)
          (recur (conj acc x)))
        acc))
    (finally (.close cur))))

(defn load-schema
  "Only the schema-defining attributes are scanned (AEVT), not all of EAVT; retracted
  idents (AEVT history of :db/ident) become aliases."
  [store snap]
  (schema/with-ident-aliases
   (schema/schema-from-datoms
    (into [] (mapcat #(scan-attr store snap %)) (sort schema/schema-attr-ids)))
   (ident-history (index/prefix-cursor (rocks/raw-cursor store :aevt-h snap)
                                       (codec/encode-prefix :aevt [schema/ident-attr-id])
                                       :aevt true))))

(defn- cache-epoch
  "The segment cache epoch of `store` now (-1 without a cache)."
  ^long [store]
  (if-let [c (.-segcache ^Store store)] (sc/epoch c) -1))

(defn- snapshot-epoch
  "Epoch of a db whose snapshot was taken between reading `e0` and now: -1 (never uses
  the cache) when an irregular write was in progress or happened meanwhile."
  ^long [store ^long e0]
  (sc/snapshot-taken!)
  (if (and (>= e0 0) (even? e0) (== e0 (cache-epoch store))) e0 -1))

(defn base-db
  "Db value over a fresh snapshot of `store`."
  [store]
  (let [e0 (cache-epoch store)
        snap (rocks/snapshot store)
        ep (snapshot-epoch store e0)
        releaser (->Releaser snap)]
    (try
      (let [tx (last-log-tx store snap)
            basis (if tx (codec/tx->t tx) 0)
            sch (load-schema store snap)
            db (->Base store snap basis sch (bootstrap/read-meta store snap) releaser ep)]
        (rocks/register-cleanup! db releaser)
        db)
      (catch Throwable t
        (.run ^Releaser releaser)
        (throw t)))))

(defn successor-db
  "Db value over a fresh snapshot of `store` taken right after the writer committed the
  tx that moved `prev` to `basis-t` / `meta` (writer thread only). The schema object of
  `prev` is reused unless `schema-changed?`, in which case it is reloaded from the new
  snapshot. No log or meta reads."
  [store ^Base prev basis-t meta schema-changed?]
  (let [e0 (cache-epoch store)
        snap (rocks/snapshot store)
        ep (snapshot-epoch store e0)
        releaser (->Releaser snap)]
    (try
      (let [sch (if schema-changed? (load-schema store snap) (.-schema prev))
            db (->Base store snap basis-t sch meta releaser ep)]
        (rocks/register-cleanup! db releaser)
        db)
      (catch Throwable t
        (.run ^Releaser releaser)
        (throw t)))))
