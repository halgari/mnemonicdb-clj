(ns mnemonic.index-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.codec :as codec]
            [mnemonic.datom :as datom]
            [mnemonic.index :as ix]
            [mnemonic.rocks :as rocks]))

(deftype VecCursor [ks ds ^:unsynchronized-mutable ^long i
                    ^:unsynchronized-mutable ^boolean closed]
  mnemonic.index.ICursor
  (valid [_] (and (not closed) (< i (count ks))))
  (advance [_] (set! i (inc i)) nil)
  (key [_] (nth ks i))
  (datom [_] (nth ds i))
  (retractTx [_] -1)
  (close [_] (set! closed (boolean true)) nil))

(defn- vcur [es]
  (let [ds (mapv #(datom/datom % 1 "v" 5 true) (sort es))]
    (->VecCursor (mapv #(codec/encode-key :eavt (:e %) (:a %) (:v %) (:tx %)) ds) ds 0 false)))

(defn- drain [c]
  (loop [acc []]
    (if (.valid c)
      (let [d (.datom c)] (.advance c) (recur (conj acc (:e d))))
      acc)))

(deftest merge-test
  (let [c (ix/merge-cursors [(vcur [1 4 7]) (vcur [2 5 8 9]) (vcur [3 6])])]
    (is (= [1 2 3 4 5 6 7 8 9] (drain c)))
    (is (not (.valid c)))
    (.close c)
    (.close c))
  (is (= [] (drain (ix/merge-cursors []))))
  (is (= [1 2] (drain (ix/merge-cursors [(vcur []) (vcur [1 2])])))))

(deftest filter-test
  (is (= [2 4 6] (drain (ix/filter-cursor (vcur [1 2 3 4 5 6]) #(even? (:e %))))))
  (is (= [] (drain (ix/filter-cursor (vcur [1 3]) #(even? (:e %))))))
  (is (= [] (drain (ix/empty-cursor)))))

(defn- with-store [f]
  (let [store (rocks/open-store {:path "index-test" :mem? true})]
    (try (f store) (finally (rocks/close-store store)))))

(defn- put-eavt! [store es]
  (rocks/write! store (for [e es] [:put :eavt (codec/encode-key :eavt e 1 "v" 5) (byte-array 0)]) {}))

(deftest prefix-cursor-test
  (with-store
    (fn [store]
      (put-eavt! store [1 2 3 4])
      (let [before (.get ix/open-cursors)
            c (ix/prefix-cursor (rocks/raw-cursor store :eavt nil)
                                (codec/encode-prefix :eavt [2]) :eavt false)]
        (is (= (inc before) (.get ix/open-cursors)))
        (is (= [2] (drain c)) "stops at the end of the prefix")
        (.close c)
        (.close c)
        (is (= before (.get ix/open-cursors)) "close is idempotent")
        (is (not (.valid c)))
        (is (nil? (.datom c))))
      (let [c (ix/seek-cursor (rocks/raw-cursor store :eavt nil)
                              (codec/encode-prefix :eavt [3]) :eavt false)]
        (is (= [3 4] (drain c)))
        (.close c)))))

(defn- db-over [store]
  (reify ix/IDb
    (-view [_ idx prefix]
      (ix/prefix-cursor (rocks/raw-cursor store idx nil) prefix idx false))))

(deftest iterable-test
  (with-store
    (fn [store]
      (put-eavt! store (range 1 1201))
      (let [it (ix/datoms-iterable (db-over store) :eavt (byte-array 0))
            before (.get ix/open-cursors)]
        (testing "reduce closes deterministically, early exit too"
          (is (= 1200 (reduce (fn [n _] (inc n)) 0 it)))
          (is (= before (.get ix/open-cursors)))
          (is (= 3 (reduce (fn [n _] (if (= n 2) (reduced 3) (inc n))) 0 it)))
          (is (= before (.get ix/open-cursors))))
        (testing "lazy seq closes on exhaustion"
          (let [s (seq it)]
            (is (= (inc before) (.get ix/open-cursors)) "open while unexhausted")
            (is (= (range 1 1201) (map :e s)))
            (is (= before (.get ix/open-cursors)))))
        (testing "empty"
          (is (nil? (seq (ix/datoms-iterable (db-over store) :eavt (codec/encode-prefix :eavt [9999])))))
          (is (= before (.get ix/open-cursors))))
        (testing "sequential, iterable, copied datoms"
          (is (sequential? it))
          (is (= 1200 (count (iterator-seq (.iterator ^Iterable it)))))
          (is (= before (.get ix/open-cursors)))
          (is (= 1200 (count (into [] it)))))
        (testing "abandoned seq is closed by the Cleaner"
          ;; first chunk (512) leaves the cursor open; dropping the seq must release it
          (is (instance? mnemonic.datom.Datom (first it)))
          (loop [i 0]
            (when (and (> (.get ix/open-cursors) before) (< i 100))
              (System/gc) (Thread/sleep 20) (recur (inc i))))
          (is (= before (.get ix/open-cursors))))))))

(deftest filter-cursor-throwing-pred-closes-source-test
  (with-store
    (fn [store]
      (put-eavt! store [1 2])
      (let [before (.get ix/open-cursors)
            src (ix/prefix-cursor (rocks/raw-cursor store :eavt nil) (byte-array 0) :eavt false)]
        (is (thrown? RuntimeException
                     (ix/filter-cursor src (fn [_] (throw (RuntimeException. "boom"))))))
        (is (= before (.get ix/open-cursors)))))))
