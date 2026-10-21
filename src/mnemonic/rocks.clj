(set! *warn-on-reflection* true)

(ns mnemonic.rocks
  "Thin RocksDB wrapper: byte arrays only, no knowledge of datoms."
  (:import (java.lang.ref Cleaner)
           (java.nio.charset StandardCharsets)
           (java.util ArrayList)
           (org.rocksdb BlockBasedTableConfig BloomFilter ColumnFamilyDescriptor
                        ColumnFamilyHandle ColumnFamilyOptions DBOptions Env
                        Options ReadOptions RocksDB RocksIterator RocksMemEnv
                        Snapshot WriteBatch WriteOptions)))

(def cf-names
  [:default :eavt :aevt :avet :vaet :eavt-h :aevt-h :avet-h :vaet-h :log])

(defrecord Store [^RocksDB db cfs env closeables])

(defonce ^:private ^Cleaner cleaner (Cleaner/create))

(defn register-cleanup!
  "Run no-arg fn `f` once `obj` becomes phantom-reachable. `f` must not close over `obj`."
  [obj f]
  (.register cleaner obj ^Runnable f)
  nil)

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
            db       (RocksDB/open dbo ^String path ^java.util.List descs handles)
            cfs      (zipmap cf-names (vec handles))]
        (->Store db cfs env (vec (reverse (vec owned)))))
      (catch Throwable t
        (doseq [c (reverse (vec owned))] (close-quietly c))
        (throw t))))))

(defn close-store
  "Close handles, then the db, then options/env. Idempotent-safe on already closed natives."
  [^Store store]
  (doseq [^ColumnFamilyHandle h (vals (:cfs store))] (close-quietly h))
  (close-quietly (:db store))
  (doseq [c (:closeables store)] (close-quietly c))
  nil)

(defn destroy-store
  "Delete an on-disk store. Not for mem stores."
  [^String path]
  (with-open [o (Options.)]
    (RocksDB/destroyDB path o))
  (let [f (java.io.File. path)]
    (when (.isDirectory f)
      (let [^"[Ljava.lang.String;" kids (.list f)]
        (when (or (nil? kids) (zero? (alength kids)))
          (.delete f)))))
  nil)

(defn snapshot ^Snapshot [^Store store]
  (.getSnapshot ^RocksDB (:db store)))

(defn release-snapshot [^Store store ^Snapshot snap]
  (.releaseSnapshot ^RocksDB (:db store) snap)
  nil)

(defn- cf-handle ^ColumnFamilyHandle [^Store store cf]
  (or (get (:cfs store) cf)
      (throw (ex-info "Unknown column family" {:cf cf}))))

(defn get-bytes
  "Point lookup. `snap` may be nil."
  ^bytes [^Store store cf ^Snapshot snap ^bytes k]
  (let [^RocksDB db (:db store)
        h (cf-handle store cf)]
    (if snap
      (with-open [ro (doto (ReadOptions.) (.setSnapshot snap))]
        (.get db h ro k))
      (.get db h k))))

(defn write!
  "Atomically apply `ops`: seq of [:put cf k v] / [:delete cf k]."
  [^Store store ops {:keys [sync?]}]
  (with-open [wb (WriteBatch.)
              wo (doto (WriteOptions.) (.setSync (boolean sync?)))]
    (doseq [op ops]
      (case (nth op 0)
        :put    (.put wb (cf-handle store (nth op 1)) ^bytes (nth op 2) ^bytes (nth op 3))
        :delete (.delete wb (cf-handle store (nth op 1)) ^bytes (nth op 2))))
    (.write ^RocksDB (:db store) wo wb))
  nil)

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
  (^void close []))

(deftype RawCursor [^RocksIterator it ^ReadOptions ro ^:unsynchronized-mutable closed?]
  IRawCursor
  (seek [_ k] (.seek it ^bytes k) nil)
  (seekToFirst [_] (.seekToFirst it) nil)
  (seekToLast [_] (.seekToLast it) nil)
  (seekForPrev [_ k] (.seekForPrev it ^bytes k) nil)
  (isValid [_] (.isValid it))
  (next [_] (.next it) nil)
  (prev [_] (.prev it) nil)
  (key [_] (.key it))
  (value [_] (.value it))
  (close [_]
    (when-not closed?
      (set! closed? true)
      (.close it)
      (.close ro))
    nil)
  java.lang.AutoCloseable)

(defn raw-cursor
  "New cursor over `cf`, reading from `snap` (may be nil). Caller must close it."
  ^RawCursor [^Store store cf ^Snapshot snap]
  (let [ro (ReadOptions.)]
    (try
      (when snap (.setSnapshot ro snap))
      (->RawCursor (.newIterator ^RocksDB (:db store) (cf-handle store cf) ro) ro false)
      (catch Throwable t
        (.close ro)
        (throw t)))))
