(set! *warn-on-reflection* true)

(ns mnemonic.rocks
  "Thin RocksDB wrapper: byte arrays only, no knowledge of datoms.

  Every native call goes through this namespace and is guarded so that no public path
  can touch a closed or freed RocksDB object (which crashes the JVM):

  - Store-level calls (new iterator, get, write, snapshot create/free) run under the
    read side of the store's ReentrantReadWriteLock and check the `closed` flag first.
    `close-store` takes the write side to set `closed` (waiting for in-flight calls),
    then closes every registered cursor and frees every registered snapshot, and only
    then closes the native db. After that every call throws
    :db.error/connection-released.
  - Cursors (RawCursor) serialize their own native calls on their monitor and check a
    closed flag first; close-store closes them under that same monitor. Cursors are
    registered with the store while open.
  - Snapshots (Snap) are reference counted: every open cursor holds a reference.
    `release-snapshot` marks the snapshot released (new cursors then throw
    :db.error/db-released) and the native snapshot is freed once no cursor uses it."
  (:import (java.lang.ref Cleaner)
           (java.nio.charset StandardCharsets)
           (java.util ArrayList Set)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.concurrent.atomic AtomicBoolean AtomicLong)
           (java.util.concurrent.locks Lock ReentrantReadWriteLock)
           (org.rocksdb BlockBasedTableConfig BloomFilter ColumnFamilyDescriptor
                        ColumnFamilyHandle ColumnFamilyOptions DBOptions Env
                        Options ReadOptions RocksDB RocksDBException RocksIterator RocksMemEnv
                        Snapshot WriteBatch WriteOptions)))

(def cf-names
  [:default :eavt :aevt :avet :vaet :eavt-h :aevt-h :avet-h :vaet-h :log])

;; guard: ReentrantReadWriteLock; closed: AtomicBoolean; cursors / snaps: concurrent sets
(defrecord Store [^RocksDB db cfs env closeables guard closed cursors snaps])

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

(defmacro ^:private guarded
  "Runs body under the read side of the store guard; throws connection-released when
  the store is closed."
  [store & body]
  `(let [^Store s# ~store
         ^Lock l# (.readLock ^ReentrantReadWriteLock (.-guard s#))]
     (.lock l#)
     (try
       (when (.get ^AtomicBoolean (.-closed s#)) (throw (connection-released-ex)))
       ~@body
       (finally (.unlock l#)))))

(defn- cf-bytes ^bytes [kw]
  (.getBytes ^String (name kw) StandardCharsets/UTF_8))

(defn- close-quietly [^java.lang.AutoCloseable c]
  (when c
    (try (.close c) (catch Throwable _ nil))))

(defn open-store
  "Open (creating if needed) a store with all column families.
  `mem? true` uses an in-memory RocksMemEnv and treats `path` as a virtual name."
  [{:keys [path mem?]}]
  (RocksDB/loadLibrary)
  ;; RocksMemEnv cannot resolve relative names (GetAbsolutePath fails), so virtual
  ;; names are rooted with a leading "/".
  (let [path (if (and mem? (not (.startsWith ^String path "/"))) (str "/" path) path)]
  (let [owned (ArrayList.)                       ; closed after db, in reverse order
        own   (fn [x] (.add owned x) x)]
    (try
      (let [env      (when mem? (own (RocksMemEnv. (Env/getDefault))))
            dbo      (own (doto (DBOptions.)
                            (.setCreateIfMissing true)
                            (.setCreateMissingColumnFamilies true)))
            _        (when env (.setEnv ^DBOptions dbo ^Env env))
            table    (BlockBasedTableConfig.)
            _        (.setFilterPolicy table (own (BloomFilter. 10.0 false)))
            _        (.setWholeKeyFiltering table true)
            cfo      (own (doto (ColumnFamilyOptions.)
                            (.setTableFormatConfig table)))
            descs    (mapv #(ColumnFamilyDescriptor. (cf-bytes %) cfo) cf-names)
            handles  (ArrayList.)
            db       (wrap-rocks (RocksDB/open dbo ^String path ^java.util.List descs handles))
            cfs      (zipmap cf-names (vec handles))]
        (->Store db cfs env (vec (reverse (vec owned)))
                 (ReentrantReadWriteLock.) (AtomicBoolean. false)
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

;; ------------------------------------------------------------------ snapshots

(definterface ISnap
  (^org.rocksdb.Snapshot acquire [])
  (^void unref [])
  (^void markReleased [])
  (^boolean isReleased [])
  (^void free []))

(deftype Snap [^Store store ^Snapshot native ^AtomicBoolean freed
               ^AtomicLong state]   ; bit 0: released; bits 1..: open cursor count
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
            (when (zero? s) (.free this))
            (recur))))))
  (isReleased [_] (== 1 (bit-and (.get state) 1)))
  (free [this]
    ;; Never under the snap monitor (lock order: store guard -> snap monitor).
    (let [^Lock l (.readLock ^ReentrantReadWriteLock (.-guard store))]
      (.lock l)
      (try
        ;; After close-store started, close-store frees every registered snapshot.
        (when (and (not (closed? store)) (.compareAndSet freed false true))
          (.remove ^Set (.-snaps store) this)
          (.releaseSnapshot ^RocksDB (.-db store) native))
        (finally (.unlock l))))))

(defn snapshot
  "A reference-counted snapshot of `store` (see `release-snapshot`)."
  ^mnemonic.rocks.Snap [^Store store]
  (guarded store
    (let [s (->Snap store (.getSnapshot ^RocksDB (.-db store)) (AtomicBoolean. false) (AtomicLong. 0))]
      (.add ^Set (.-snaps store) s)
      s)))

(defn release-snapshot
  "Marks `snap` released: new reads through it throw :db.error/db-released; the native
  snapshot is freed once no open cursor uses it. Idempotent, safe after close-store."
  [_store ^Snap snap]
  (.markReleased snap)
  nil)

(defn snapshot-released? [^Snap snap] (.isReleased snap))

;; ------------------------------------------------------------------ point ops

(defn get-bytes
  "Point lookup. `snap` (a Snap) may be nil."
  ^bytes [^Store store cf snap ^bytes k]
  (guarded store
    (let [^RocksDB db (.-db store)
          h (cf-handle store cf)]
      (if snap
        (let [^Snap snap snap
              native (.acquire snap)]
          (try
            (with-open [ro (doto (ReadOptions.) (.setSnapshot native))]
              (wrap-rocks (.get db h ro k)))
            (finally
              ;; unref may free, which takes the read lock again (reentrant)
              (.unref snap))))
        (wrap-rocks (.get db h k))))))

(defn write!
  "Atomically apply `ops`: seq of [:put cf k v] / [:delete cf k]."
  [^Store store ops {:keys [sync?]}]
  (with-open [wb (WriteBatch.)
              wo (doto (WriteOptions.) (.setSync (boolean sync?)))]
    ;; the batch references column family handles: build it under the guard too
    (guarded store
      (doseq [op ops]
        (case (nth op 0)
          :put    (.put wb (cf-handle store (nth op 1)) ^bytes (nth op 2) ^bytes (nth op 3))
          :delete (.delete wb (cf-handle store (nth op 1)) ^bytes (nth op 2))))
      (wrap-rocks (.write ^RocksDB (.-db store) wo wb))))
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
  (^void close [])
  (^void forceClose []))

(defn- check-status
  "Called when the iterator became invalid: an I/O error or corruption must not look
  like the end of the data."
  [^RocksIterator it]
  (wrap-rocks (.status it)))

(defn- throw-closed [state]
  (if (= state :store-closed)
    (throw (connection-released-ex))
    (throw (ex-info "Cursor is closed" {:db/error :db.error/cursor-closed
                                        :cognitect.anomalies/category :cognitect.anomalies/incorrect}))))

(deftype RawCursor [^Store store ^RocksIterator it ^ReadOptions ro snap
                    ^:unsynchronized-mutable state]   ; nil open, :closed, :store-closed
  IRawCursor
  (seek [this k] (locking this (when state (throw-closed state)) (.seek it ^bytes k) nil))
  (seekToFirst [this] (locking this (when state (throw-closed state)) (.seekToFirst it) nil))
  (seekToLast [this] (locking this (when state (throw-closed state)) (.seekToLast it) nil))
  (seekForPrev [this k] (locking this (when state (throw-closed state)) (.seekForPrev it ^bytes k) nil))
  (isValid [this]
    (locking this
      (when state (throw-closed state))
      (or (.isValid it) (do (check-status it) false))))
  (next [this] (locking this (when state (throw-closed state)) (.next it) nil))
  (prev [this] (locking this (when state (throw-closed state)) (.prev it) nil))
  (key [this] (locking this (when state (throw-closed state)) (.key it)))
  (value [this] (locking this (when state (throw-closed state)) (.value it)))
  (seekKey [this k]
    (locking this
      (when state (throw-closed state))
      (.seek it ^bytes k)
      (if (.isValid it) (.key it) (do (check-status it) nil))))
  (nextKey [this]
    (locking this
      (when state (throw-closed state))
      (.next it)
      (if (.isValid it) (.key it) (do (check-status it) nil))))
  (close [this]
    (locking this
      (when (nil? state)
        (set! state :closed)
        (.close it)
        (.close ro)
        (.remove ^Set (.-cursors store) this)
        ;; may free the snapshot (store read lock; lock order: cursor -> store guard)
        (when snap (.unref ^Snap snap)))
      nil))
  (forceClose [this]
    (locking this
      (when (nil? state)
        (set! state :store-closed)
        (.close it)
        (.close ro))
      nil))
  java.lang.AutoCloseable)

(defn raw-cursor
  "New cursor over `cf`, reading from `snap` (a Snap, may be nil). Caller must close it.
  Returns a RawCursor (hint call sites with ^mnemonic.rocks.IRawCursor)."
  ^mnemonic.rocks.RawCursor [^Store store cf snap]
  (guarded store
    (let [native (when snap (.acquire ^Snap snap))
          ro (ReadOptions.)]
      (try
        (when native (.setSnapshot ro native))
        (let [c (->RawCursor store (.newIterator ^RocksDB (.-db store) (cf-handle store cf) ro) ro snap nil)]
          (.add ^Set (.-cursors store) c)
          c)
        (catch Throwable t
          (.close ro)
          (when snap (.unref ^Snap snap))
          (throw t))))))

;; ------------------------------------------------------------------ close

(defn close-store
  "Closes `store`: waits for in-flight native calls, then closes every open cursor and
  frees every snapshot, then closes handles, the db and options. Afterwards every
  operation on the store, its cursors and its snapshots throws
  :db.error/connection-released. Idempotent."
  [^Store store]
  (let [^Lock wl (.writeLock ^ReentrantReadWriteLock (.-guard store))
        first? (do (.lock wl)
                   (try (.compareAndSet ^AtomicBoolean (.-closed store) false true)
                        (finally (.unlock wl))))]
    (when first?
      (doseq [^IRawCursor c (vec (.-cursors store))]
        (try (.forceClose c) (catch Throwable _ nil)))
      (.clear ^Set (.-cursors store))
      (doseq [^Snap s (vec (.-snaps store))]
        (when (.compareAndSet ^AtomicBoolean (.-freed s) false true)
          (try (.releaseSnapshot ^RocksDB (.-db store) ^Snapshot (.-native s))
               (catch Throwable _ nil))))
      (.clear ^Set (.-snaps store))
      (doseq [^ColumnFamilyHandle h (vals (:cfs store))] (close-quietly h))
      (close-quietly (.-db store))
      (doseq [c (:closeables store)] (close-quietly c))))
  nil)
