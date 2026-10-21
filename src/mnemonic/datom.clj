(ns mnemonic.datom
  "The Datom type: [e a v tx added], Datomic-compatible lookup and destructuring.")

(set! *warn-on-reflection* true)

(defn value=
  "Value equality consistent with the storage encoding: byte arrays by content,
  BigDecimals exactly (value and scale), everything else with `=`."
  [a b]
  (cond
    (bytes? a) (and (bytes? b) (java.util.Arrays/equals ^bytes a ^bytes b))
    (instance? java.math.BigDecimal a) (.equals ^java.math.BigDecimal a b)
    :else (= a b)))

(defn value-hash
  "Hash consistent with `value=`."
  ^long [v]
  (cond
    (bytes? v) (java.util.Arrays/hashCode ^bytes v)
    (instance? java.math.BigDecimal v) (.hashCode ^java.math.BigDecimal v)
    :else (hash v)))

(defmacro ^:private datom-hash
  "Same value as `(hash [e a (value-hash v) tx added])` without building the vector
  (a macro because fns cannot take five primitive args)."
  [e a v tx added]
  `(let [h# (unchecked-add-int (unchecked-multiply-int 31 1) (clojure.lang.Murmur3/hashLong ~e))
         h# (unchecked-add-int (unchecked-multiply-int 31 h#) (clojure.lang.Murmur3/hashLong ~a))
         h# (unchecked-add-int (unchecked-multiply-int 31 h#) (clojure.lang.Murmur3/hashLong (value-hash ~v)))
         h# (unchecked-add-int (unchecked-multiply-int 31 h#) (clojure.lang.Murmur3/hashLong ~tx))
         h# (unchecked-add-int (unchecked-multiply-int 31 h#) (if ~added 1231 1237))]
     (clojure.lang.Murmur3/mixCollHash h# 5)))

(deftype Datom [^long e ^long a v ^long tx ^boolean added]
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [_ k nf]
    (case k
      :e e
      :a a
      :v v
      :tx tx
      :t tx
      :added added
      nf))

  clojure.lang.Indexed
  (nth [_ i]
    (case (int i)
      0 e
      1 a
      2 v
      3 tx
      4 added
      (throw (IndexOutOfBoundsException. (str "Datom index out of range: " i)))))
  (nth [this i nf]
    (if (and (>= i 0) (< i 5)) (.nth this i) nf))

  clojure.lang.Counted
  (count [_] 5)

  clojure.lang.Seqable
  (seq [_] (list e a v tx added))

  clojure.lang.IHashEq
  (hasheq [_] (datom-hash e a v tx added))

  Object
  (hashCode [_] (datom-hash e a v tx added))
  (equals [_ o]
    (and (instance? Datom o)
         (let [^Datom o o]
           (and (== e (.-e o)) (== a (.-a o)) (== tx (.-tx o))
                (= added (.-added o)) (value= v (.-v o))))))
  (toString [_] (str "#datom[" (pr-str e) " " (pr-str a) " " (pr-str v) " "
                     (pr-str tx) " " (pr-str added) "]")))

(defn datom
  "Construct a Datom."
  [e a v tx added]
  (Datom. e a v tx (boolean added)))

(defmethod print-method Datom [x ^java.io.Writer w]
  (.write w (str x)))

(defmethod print-dup Datom [x ^java.io.Writer w]
  (.write w (str x)))
