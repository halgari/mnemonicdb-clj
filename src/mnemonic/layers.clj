(ns mnemonic.layers
  "Composable db layers over the base db: TimeView (as-of / since / history), Filter and
  With (speculative overlay)."
  (:refer-clojure :exclude [filter])
  (:require [mnemonic.codec :as codec]
            [mnemonic.datom :as datom]
            [mnemonic.db :as db]
            [mnemonic.index :as index]
            [mnemonic.schema :as schema]
            [mnemonic.tx :as tx])
  (:import (java.util Arrays Comparator Date Iterator Map$Entry NavigableMap TreeMap)
           (mnemonic.datom Datom)
           (mnemonic.db Base)
           (mnemonic.index ICursor)))

(set! *warn-on-reflection* true)

(defn- unsupported [msg data]
  (throw (ex-info msg (merge {:db/error :db.error/unsupported
                              :cognitect.anomalies/category :cognitect.anomalies/unsupported}
                             data))))

(def ^:private history-cf {:eavt :eavt-h :aevt :aevt-h :avet :avet-h :vaet :vaet-h})

;; ------------------------------------------------------------------ sources

(defprotocol ISource
  "Raw access to the current / history entries of a base-like db (Base or With).
  Exactly one of `start` (seek: first key >= start, to the end of the index) and
  `prefix` is non-nil. History entries report their retracting tx via retractTx."
  (-open [db idx hist? start prefix]))

(extend-type Base
  ISource
  (-open [b idx hist? start prefix]
    (cond
      (and (not hist?) prefix) (index/-view b idx prefix)
      (not hist?) (index/-seek-view b idx start)
      prefix (index/-seek-history b idx prefix)
      :else (db/history-seek-cursor b idx start))))

;; ------------------------------------------------------------------ overlay (With)

(def ^:private ^Comparator byte-cmp
  (reify Comparator
    (compare [_ a b] (Arrays/compareUnsigned ^bytes a ^bytes b))))

(def ^:private deleted ::deleted)

(defn- next-entry [^Iterator it]
  (when (.hasNext it) (.next it)))

(defn- settle!
  "Positions the merge of `inner` and the overlay iterator on its next entry. Returns
  1 (inner is current), 2 (the overlay entry in box[0]) or 0 (exhausted). The overlay wins
  on equal keys; overlay deletes hide inner entries."
  ^long [^ICursor inner ^Iterator it ^objects box]
  (loop []
    (let [^Map$Entry oe (aget box 0)
          iv (.valid inner)]
      (cond
        (and (nil? oe) (not iv)) 0
        (nil? oe) 1
        (not iv) (if (identical? deleted (.getValue oe))
                   (do (aset box 0 (next-entry it)) (recur))
                   2)
        :else
        (let [^bytes ok (.getKey oe)
              ;; inner's key compared in place (no copy per entry)
              c (Arrays/compareUnsigned (index/key-buf inner) 0 (unchecked-int (index/key-len inner))
                                        ok 0 (alength ok))]
          (cond
            (neg? c) 1
            (zero? c) (do (.advance inner) (recur))
            (identical? deleted (.getValue oe)) (do (aset box 0 (next-entry it)) (recur))
            :else 2))))))

(deftype OverlayCursor [^ICursor inner ^Iterator it ^objects box idx hist?
                        ^:unsynchronized-mutable ^long src
                        ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (pos? src))
  (advance [_]
    (case src
      1 (.advance inner)
      2 (aset box 0 (next-entry it))
      nil)
    (when (pos? src)
      (set! src (settle! inner it box)))
    nil)
  (key [_]
    (case src
      1 (.key inner)
      2 (.getKey ^Map$Entry (aget box 0))
      nil))
  (datom [this]
    (case src
      1 (.datom inner)
      2 (codec/decode-key idx (.key this))
      nil))
  (retractTx [_]
    (case src
      1 (.retractTx inner)
      2 (if hist?
          (codec/bytes->long (.getValue ^Map$Entry (aget box 0)))
          -1)
      -1))
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (set! src 0)
      (.close inner))
    nil)
  ;; inner entries pass their views and key buffer through; overlay entries are Datoms
  mnemonic.index.IViewCursor
  (view [this]
    (case src
      1 (index/view-of inner)
      2 (.datom this)
      nil))
  mnemonic.index.IKeyView
  (keyBuf [_]
    (case src
      1 (index/key-buf inner)
      2 (.getKey ^Map$Entry (aget box 0))
      nil))
  (keyLen [_]
    (case src
      1 (index/key-len inner)
      2 (alength ^bytes (.getKey ^Map$Entry (aget box 0)))
      0)))

(defn- overlay-cursor
  [^ICursor inner ^TreeMap tm idx hist? ^bytes start ^bytes prefix]
  (let [from (or start prefix)
        end (when prefix (codec/prefix-end prefix))
        ^NavigableMap slice (if end
                              (.subMap tm from true end false)
                              (.tailMap tm from true))
        it (.iterator (.entrySet slice))
        box (object-array 1)]
    (try
      (aset box 0 (next-entry it))
      (->OverlayCursor inner it box idx hist? (settle! inner it box) false)
      (catch Throwable t
        (.close inner)
        (throw t)))))

;; overlay: {cf TreeMap<byte[] -> byte[] | ::deleted>}
(deftype With [inner overlay basis-t schema meta]
  ISource
  (-open [_ idx hist? start prefix]
    (let [^TreeMap tm (get overlay (if hist? (history-cf idx) idx))
          cur (-open inner idx hist? start prefix)]
      (if (or (nil? tm) (.isEmpty tm))
        cur
        (overlay-cursor cur tm idx hist? start prefix))))
  index/IDb
  (-seek-current [this idx prefix] (-open this idx false nil prefix))
  (-seek-history [this idx prefix] (-open this idx true nil prefix))
  (-view [this idx prefix] (-open this idx false nil prefix))
  (-seek-view [this idx start] (-open this idx false start nil))
  (-basis-t [_] basis-t)
  (-schema [_] schema)
  (-as-of-t [_] nil)
  (-since-t [_] nil)
  (-history? [_] false)
  (-filter-pred [_] nil)
  (-raw [_] (index/-raw inner)))

(defn- new-overlay []
  (into {} (map (fn [cf] [cf (TreeMap. byte-cmp)]))
        (concat [:eavt :aevt :avet :vaet] (vals history-cf))))

(defn- apply-ops! [overlay ops]
  (doseq [op ops]
    (let [cf (nth op 1)]
      (when-let [^TreeMap tm (get overlay cf)]
        (case (nth op 0)
          :put (.put tm (nth op 2) (nth op 3))
          :delete (.put tm (nth op 2) deleted))))))

(defn- load-schema
  "Schema of `w`, scanned through its own (overlaid) AEVT."
  [w]
  (schema/with-ident-aliases
   (schema/schema-from-datoms
    (into [] (mapcat #(index/datoms-iterable w :aevt (codec/encode-prefix :aevt [%])))
          (sort schema/schema-attr-ids)))
   (db/ident-history (index/-seek-history w :aevt (codec/encode-prefix :aevt [schema/ident-attr-id])))))

(defn with
  "Applies `tx-data` speculatively to `db*`; returns {:db-before :db-after :tx-data :tempids}.
  Nothing is written."
  [db* tx-data]
  (when-not (or (instance? Base db*) (instance? With db*))
    (unsupported "with is only supported on a current (not as-of, since, history or filtered) db"
                 {:feature "with"}))
  (let [meta (if (instance? With db*) (.-meta ^With db*) (db/db-meta db*))
        r (tx/prepare db* meta tx-data)
        overlay (new-overlay)
        _ (apply-ops! overlay (:ops r))
        basis (codec/tx->t (long (:tx r)))
        w0 (->With db* overlay basis (index/-schema db*) (:meta r))
        w (->With db* overlay basis (load-schema w0) (:meta r))]
    {:db-before db* :db-after w :tx-data (:tx-data r) :tempids (:tempids r)}))

;; ------------------------------------------------------------------ filter

(deftype Filter [inner pred]
  index/IDb
  (-seek-current [_ idx prefix]
    (index/filter-cursor (index/-seek-current inner idx prefix) #(pred inner %)))
  (-seek-history [_ idx prefix]
    (index/filter-cursor (index/-seek-history inner idx prefix) #(pred inner %)))
  (-view [_ idx prefix]
    (index/filter-cursor (index/-view inner idx prefix) #(pred inner %)))
  (-seek-view [_ idx start]
    (index/filter-cursor (index/-seek-view inner idx start) #(pred inner %)))
  (-basis-t [_] (index/-basis-t inner))
  (-schema [_] (index/-schema inner))
  (-as-of-t [_] (index/-as-of-t inner))
  (-since-t [_] (index/-since-t inner))
  (-history? [_] (index/-history? inner))
  (-filter-pred [_] pred)
  (-raw [_] (index/-raw inner)))

;; ------------------------------------------------------------------ time views

(defn- key-tx-len
  "The tx of the index key in the first `n` bytes of `k`: its trailing 8 bytes
  (big-endian, sign bit flipped)."
  ^long [^bytes k ^long n]
  (bit-xor (.getLong (java.nio.ByteBuffer/wrap k) (unchecked-int (- n 8))) Long/MIN_VALUE))

(defn- key-tx
  "The tx of an index key: its trailing 8 bytes (big-endian, sign bit flipped)."
  ^long [^bytes k]
  (key-tx-len k (alength k)))

(defn- key-with-tx
  "Copy of index key `k` with its trailing tx replaced by `tx`."
  ^bytes [^bytes k ^long tx]
  (let [n (alength k)
        r (Arrays/copyOf k n)
        x (bit-xor tx Long/MIN_VALUE)]
    (dotimes [i 8]
      (aset r (+ (- n 8) i) (unchecked-byte (unsigned-bit-shift-right x (* 8 (- 7 i))))))
    r))

(defmacro ^:private visible-entry?
  [c lo hi]
  `(let [tx# (key-tx-len (index/key-buf ~c) (index/key-len ~c))
         rtx# (.retractTx ~c)]
     (and (> tx# ~lo) (<= tx# ~hi) (or (neg? rtx#) (< ~hi rtx#)))))

(deftype VisibleCursor [^ICursor c ^long lo ^long hi
                        ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (and (not closed?) (.valid c)))
  (advance [_]
    (when-not closed?
      (.advance c)
      (loop []
        (when (and (.valid c) (not (visible-entry? c lo hi)))
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
  ;; flyweight views and the key buffer pass through (read only while valid)
  mnemonic.index.IViewCursor
  (view [_] (index/view-of c))
  mnemonic.index.IKeyView
  (keyBuf [_] (index/key-buf c))
  (keyLen [_] (index/key-len c)))

(defn- visible-cursor
  "Entries visible in a non-history view with since < tx <= as-of: current entries by
  their tx, history entries when they were asserted by as-of and retracted after it."
  [^ICursor c lo hi]
  (let [lo (long lo) hi (long hi)]
    (try
      (loop []
        (when (and (.valid c) (not (visible-entry? c lo hi)))
          (.advance c)
          (recur)))
      (catch Throwable t
        (.close c)
        (throw t)))
    (->VisibleCursor c lo hi false)))

(deftype Event [^bytes key ^Datom d ^long tx ^boolean added])

(defn- same-group?
  "Keys equal except for the trailing 8-byte tx (`b`: the first `bn` bytes)."
  [^bytes a ^bytes b ^long bn]
  (let [n (alength a)]
    (and (== n bn)
         (Arrays/equals a 0 (- n 8) b 0 (- n 8)))))

(defn- event-before?
  "Newest first; an assert before a retract on equal tx."
  [^Event a ^Event b]
  (let [ta (.-tx a) tb (.-tx b)]
    (or (> ta tb)
        (and (== ta tb) (.-added a) (not (.-added b))))))

(defn- sort-events!
  "Stable insertion sort of the first `n` events (groups are small)."
  [^objects evs ^long n]
  (loop [i 1]
    (when (< i n)
      (let [x (aget evs i)]
        (loop [j (dec i)]
          (if (and (>= j 0) (event-before? x (aget evs j)))
            (do (aset evs (inc j) (aget evs j)) (recur (dec j)))
            (aset evs (inc j) x))))
      (recur (inc i))))
  evs)

(defn- read-group
  "Consumes the next group of source entries sharing (index key minus tx) and returns its
  events with since < tx <= as-of, newest first (an assert before a retract on equal tx).
  Visibility is decided from the key's tx bytes; a datom is decoded only when emitted.
  `scratch` is a one-element array holding a reusable, growable event buffer."
  ^objects [^ICursor src ^objects scratch ^long lo ^long hi]
  (loop []
    (if-not (.valid src)
      (object-array 0)
      (let [k0 (.key src)
            n (loop [n 0]
                (if (and (.valid src) (same-group? k0 (index/key-buf src) (index/key-len src)))
                  (let [tx (key-tx-len (index/key-buf src) (index/key-len src))
                        rtx (.retractTx src)
                        a? (and (> tx lo) (<= tx hi))
                        r? (and (>= rtx 0) (> rtx lo) (<= rtx hi))]
                    (if (or a? r?)
                      (let [k (.key src)
                            ^Datom d (.datom src)
                            evs (let [^objects evs (aget scratch 0)]
                                  (if (<= (+ n 2) (alength evs))
                                    evs
                                    (let [g (Arrays/copyOf evs (* 2 (alength evs)))]
                                      (aset scratch 0 g)
                                      g)))
                            n (if a?
                                (do (aset evs n (->Event k d tx true)) (inc n))
                                n)
                            n (if r?
                                (do (aset evs n (->Event (key-with-tx k rtx)
                                                         (datom/datom (.-e d) (.-a d) (.-v d) rtx false)
                                                         rtx false))
                                    (inc n))
                                n)]
                        (.advance src)
                        (recur n))
                      (do (.advance src) (recur n))))
                  n))]
        (if (zero? n)
          (recur)
          (Arrays/copyOf ^objects (sort-events! (aget scratch 0) n) (int n)))))))

(deftype HistoryCursor [^ICursor src ^objects scratch ^long lo ^long hi
                        ^:unsynchronized-mutable ^objects buf
                        ^:unsynchronized-mutable ^long pos
                        ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (< pos (alength buf)))
  (advance [_]
    (when (< pos (alength buf))
      (set! pos (inc pos))
      (when (>= pos (alength buf))
        (set! buf (read-group src scratch lo hi))
        (set! pos 0)))
    nil)
  (key [_] (when (< pos (alength buf)) (.key ^Event (aget buf pos))))
  (datom [_] (when (< pos (alength buf)) (.d ^Event (aget buf pos))))
  (retractTx [_] -1)
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (set! buf (object-array 0))
      (.close src))
    nil))

(defn- history-cursor [^ICursor src idx lo hi]
  (let [lo (long lo) hi (long hi)
        scratch (doto (object-array 1) (aset 0 (object-array 16)))]
    (try
      (->HistoryCursor src scratch lo hi (read-group src scratch lo hi) 0 false)
      (catch Throwable t
        (.close src)
        (throw t)))))

(declare ->TimeView)

(defn- time-cursor [inner idx start prefix as-of since history?]
  (let [lo (if since (codec/t->tx (long since)) Long/MIN_VALUE)
        hi (if as-of (codec/t->tx (long as-of)) Long/MAX_VALUE)
        cur (-open inner idx false start prefix)
        src (if (or history? as-of)
              (try (index/merge-cursors [cur (-open inner idx true start prefix)])
                   (catch Throwable t (.close ^ICursor cur) (throw t)))
              cur)]
    (if history?
      (history-cursor src idx lo hi)
      (visible-cursor src lo hi))))

;; A time-restricted view. Reads go through -view / -seek-view, which apply the as-of /
;; since / history bounds.
;;
;; WARNING: -seek-current and -seek-history are raw pass-throughs to the inner db and
;; BYPASS the time bounds (they expose the inner db's current and history entries as
;; stored). They exist for code that needs the underlying entries (e.g. time-cursor
;; building on -open); never use them to answer a query against a TimeView.
(deftype TimeView [inner as-of since history?]
  index/IDb
  ;; bypasses as-of / since (see the warning above)
  (-seek-current [_ idx prefix] (index/-seek-current inner idx prefix))
  ;; bypasses as-of / since (see the warning above)
  (-seek-history [_ idx prefix] (index/-seek-history inner idx prefix))
  (-view [_ idx prefix] (time-cursor inner idx nil prefix as-of since history?))
  (-seek-view [_ idx start] (time-cursor inner idx start nil as-of since history?))
  (-basis-t [_] (index/-basis-t inner))
  (-schema [_] (index/-schema inner))
  (-as-of-t [_] as-of)
  (-since-t [_] since)
  (-history? [_] history?)
  (-filter-pred [_] nil)
  (-raw [_] (index/-raw inner)))

(def ^:private t-base (codec/t->tx 0))

(defn- first-tx-at-or-after
  "The first :db/txInstant datom of `db*` whose instant is >= `date` (AVET seek; ties in
  tx order), or nil."
  ^Datom [db* ^Date date]
  (let [^ICursor c (index/-seek-view db* :avet (codec/encode-prefix :avet [50 date]))]
    (try
      (when (.valid c)
        (let [^Datom d (.datom c)]
          (when (== 50 (.-a d)) (index/copy-datom d))))
      (finally (.close c)))))

(defn- date->t
  "Datomic's conversion of a Date to a t: T is the first transaction whose
  :db/txInstant is >= `date`; the t is T's when its instant equals `date`, else T's t
  minus one; when there is no such transaction, the db's next t."
  [db* ^Date date]
  (if-let [^Datom d (first-tx-at-or-after db* date)]
    (let [t (codec/tx->t (.-e d))]
      (if (== (.getTime ^Date (.-v d)) (.getTime date)) t (dec t)))
    (long (:next-t (db/db-meta (index/-raw db*))))))

(defn- ->t
  "t from a t, a tx id or a Date."
  [db* x]
  (cond
    (instance? Date x) (date->t db* x)
    (integer? x) (let [x (long x)] (if (>= x t-base) (codec/tx->t x) x))
    :else (throw (ex-info (str "Invalid t " (pr-str x))
                          {:db/error :db.error/invalid-t
                           :cognitect.anomalies/category :cognitect.anomalies/incorrect}))))

(defn- retime
  "Applies `f` (inner as-of since history?) -> new view. A Filter keeps its predicate on top
  of the re-timed inner db."
  [db* f]
  (cond
    (instance? Filter db*) (->Filter (retime (.-inner ^Filter db*) f) (.-pred ^Filter db*))
    (instance? TimeView db*) (let [^TimeView tv db*]
                               (f (.-inner tv) (.-as-of tv) (.-since tv) (.-history? tv)))
    :else (f db* nil nil false)))

;; ------------------------------------------------------------------ public API

(defn as-of
  "`db*` as of `t` (a t, tx id or Date). The outer call wins over an earlier as-of."
  [db* t]
  (let [t (->t db* t)]
    (retime db* (fn [inner _ since h?] (->TimeView inner t since h?)))))

(defn since
  "Datoms asserted after `t` (a t, tx id or Date)."
  [db* t]
  (let [t (->t db* t)]
    (retime db* (fn [inner as-of _ h?] (->TimeView inner as-of t h?)))))

(defn history
  "All assertions and retractions ever made."
  [db*]
  (retime db* (fn [inner as-of since _] (->TimeView inner as-of since true))))

(defn filter
  "`db*` with datoms failing `(pred db datom)` hidden. Filters compose."
  [db* pred]
  (if (instance? Filter db*)
    (let [^Filter f db*
          p1 (.-pred f)]
      (->Filter (.-inner f) (fn [d datom] (and (p1 d datom) (pred d datom)))))
    (->Filter db* pred)))

(defn as-of-t [db*] (index/-as-of-t db*))
(defn since-t [db*] (index/-since-t db*))
(defn history? [db*] (boolean (index/-history? db*)))
