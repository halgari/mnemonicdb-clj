(ns mnemonic.datom
  "The Datom type: [e a v tx added], Datomic-compatible lookup and destructuring.")

(set! *warn-on-reflection* true)

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
  (hasheq [_] (hash [e a v tx added]))

  Object
  (hashCode [_] (hash [e a v tx added]))
  (equals [_ o]
    (and (instance? Datom o)
         (let [^Datom o o]
           (and (== e (.-e o)) (== a (.-a o)) (== tx (.-tx o))
                (= added (.-added o)) (= v (.-v o))))))
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
