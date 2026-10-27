(ns mnemonic.query.longmap
  "A map from primitive longs (entity ids) to groups of tuples, for hash joins keyed on
  one entity position: open addressing over a long[], no boxed key or map node per entry.")

(set! *warn-on-reflection* true)

(definterface ILongGroups
  (^void add [^long k v] "Appends `v` to the group of `k`.")
  (get [^long k] "The group of `k` (an ArrayList), or nil.")
  (^long size []))

(definterface IRehash
  (^void rehash [^long cap ^boolean mix?]))

(defn- mix ^long [^long k]
  ;; Murmur3 finalizer
  (let [k (bit-xor k (unsigned-bit-shift-right k 33))
        k (unchecked-multiply k (unchecked-long 0xff51afd7ed558ccd))
        k (bit-xor k (unsigned-bit-shift-right k 33))
        k (unchecked-multiply k (unchecked-long 0xc4ceb9fe1a85ec53))]
    (bit-xor k (unsigned-bit-shift-right k 33))))

(def ^:private ^:const max-probe
  "An insert that probes more slots than this switches the table to mixed hashing."
  32)

;; Keys sit at their own low bits while that spreads them (entity ids are mostly
;; sequential, and a join probes them in index order, so neighbouring ids are
;; neighbouring slots); a cluster (strided ids) switches to the Murmur mix for good.
(deftype LongGroups [^:unsynchronized-mutable ^longs ks
                     ^:unsynchronized-mutable ^objects vs
                     ^:unsynchronized-mutable ^long n
                     ^:unsynchronized-mutable ^long mask
                     ^:unsynchronized-mutable ^boolean mixed]
  ILongGroups
  (add [this k v]
    (when (>= (* 2 (inc n)) (alength ks))
      (.rehash this (* 2 (alength ks)) mixed))
    (loop [j (bit-and (if mixed (mix k) k) mask) probes 0]
      (let [g (aget vs j)]
        (cond
          (nil? g) (if (and (not mixed) (> probes (long max-probe)))
                     (do (.rehash this (alength ks) true)
                         (.add this k v))
                     (let [l (java.util.ArrayList. 2)]
                       (.add l v)
                       (aset ks j k)
                       (aset vs j l)
                       (set! n (inc n))))
          (== k (aget ks j)) (.add ^java.util.ArrayList g v)
          :else (recur (bit-and (inc j) mask) (inc probes)))))
    nil)
  (get [_ k]
    (loop [j (bit-and (if mixed (mix k) k) mask)]
      (let [g (aget vs j)]
        (cond
          (nil? g) nil
          (== k (aget ks j)) g
          :else (recur (bit-and (inc j) mask))))))
  (size [_] n)
  IRehash
  (rehash [_ cap mix?]
    (let [oks ks ovs vs]
      (set! ks (long-array cap))
      (set! vs (object-array cap))
      (set! mask (dec cap))
      (set! mixed (boolean mix?))
      (dotimes [i (alength oks)]
        (when-let [g (aget ovs i)]
          (let [ok (aget oks i)]
            (loop [j (bit-and (if mixed (mix ok) ok) mask)]
              (if (nil? (aget vs j))
                (do (aset ks j ok) (aset vs j g))
                (recur (bit-and (inc j) mask)))))))
      nil)))

(defn long-groups
  "An empty LongGroups sized for about `expected` keys."
  ^LongGroups [^long expected]
  (let [cap (loop [c 16] (if (< c (* 2 expected)) (recur (* 2 c)) c))]
    (LongGroups. (long-array cap) (object-array cap) 0 (dec cap) false)))
