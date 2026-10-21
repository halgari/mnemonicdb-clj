(ns mnemonic.oracle
  "Differential-test helpers: run the same operations against Datomic (the oracle) and
  MnemonicDB and compare normalized results."
  (:require [datomic.api :as d]
            [mnemonic.api :as m]))

(defmacro with-both
  "Binds `sym-d` to a fresh Datomic mem connection and `sym-m` to a fresh MnemonicDB mem
  connection for the duration of `body`."
  [[sym-d sym-m] & body]
  `(let [ud# (str "datomic:mem://" (java.util.UUID/randomUUID))
         um# (str "mnemonic:mem://" (java.util.UUID/randomUUID))
         _# (d/create-database ud#)
         _# (m/create-database um#)
         ~sym-d (d/connect ud#)
         ~sym-m (m/connect um#)]
     (try
       ~@body
       (finally
         (m/release ~sym-m)
         (d/delete-database ud#)
         (m/delete-database um#)))))

(def ^:private tx-instant-attr 50)

(defn- norm-value [v]
  (cond
    (bytes? v) (vec v)
    (and (instance? Double v) (Double/isNaN v)) :NaN
    (and (instance? Float v) (Float/isNaN v)) :NaN
    (instance? Float v) (double v)
    (instance? BigDecimal v) (.stripTrailingZeros ^BigDecimal v)
    (instance? Integer v) (long v)
    (instance? Short v) (long v)
    (vector? v) (mapv norm-value v)
    :else v))

(defn normalize-datoms
  "Datoms -> vectors [e a v tx added], with :db/txInstant values replaced by :inst."
  [ds]
  (mapv (fn [d]
          (let [a (:a d)]
            [(:e d) a (if (= a tx-instant-attr) :inst (norm-value (:v d))) (:tx d) (:added d)]))
        ds))

(defn- error-kind [^Throwable t]
  (let [c (or (.getCause t) t)]
    (or (:db/error (ex-data c)) (:db/error (ex-data t)) [:unclassified (class c) (ex-message c)])))

(defn- explicit-idx
  "idx of a numeric :tempids key built from `(tempid part n)` with small negative n, else nil."
  [k]
  (when (and (integer? k) (neg? k))
    (let [low (bit-and (unchecked-subtract (long k) Long/MIN_VALUE) (dec (bit-shift-left 1 42)))
          idx (- low (bit-shift-left 1 42))]
      idx)))

(defn- normalize-tempids [tempids explicit]
  (reduce-kv (fn [acc k v]
               (cond
                 (string? k) (assoc-in acc [:named k] v)
                 (and (integer? k) (contains? explicit (explicit-idx k)))
                 (assoc-in acc [:explicit (explicit-idx k)] v)
                 :else (update acc :anon (fnil conj []) v)))
             {} tempids))

(defn- normalize-report [r explicit]
  (let [t (normalize-tempids (:tempids r) explicit)]
    {:tx-data (set (normalize-datoms (:tx-data r)))
     :tempids (update t :anon (comp vec sort))}))

(defn- run-tx [transact-fn conn tempid-fn explicit tx-data]
  (let [data (if (fn? tx-data)
               (tx-data (fn ([part] (tempid-fn part))
                          ([part n] (swap! explicit conj n) (tempid-fn part n))))
               tx-data)]
    (let [r (try {:report @(transact-fn conn data)}
                 (catch Throwable t {:error (error-kind t)}))]
      (if (contains? r :error)
        r
        (normalize-report (:report r) @explicit)))))

(defn transact-both
  "Transacts `tx-data` on both connections. `tx-data` is data, or a fn of a tempid fn
  (`(tempid part)` / `(tempid part n)`) returning data. Returns {:datomic r :mnemonic r}
  where each r is {:tx-data set-of-normalized-datoms :tempids normalized} or
  {:error :db.error/...}."
  [[conn-d conn-m] tx-data]
  (let [explicit (atom #{})]
    {:datomic (run-tx d/transact conn-d d/tempid explicit tx-data)
     :mnemonic (run-tx m/transact conn-m m/tempid explicit tx-data)}))

(defn index-dump
  "Normalized datoms of one index, in index order, for both dbs."
  [db-d db-m index & components]
  {:datomic (normalize-datoms (seq (apply d/datoms db-d index components)))
   :mnemonic (normalize-datoms (seq (apply m/datoms db-m index components)))})

(def indexes [:eavt :aevt :avet :vaet])
