(ns mnemonic.datom-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.datom :as d]))

(deftest lookup
  (let [x (d/datom 1 2 "v" 3 true)]
    (is (= 1 (:e x)))
    (is (= 2 (:a x)))
    (is (= "v" (:v x)))
    (is (= 3 (:tx x)))
    (is (= 3 (:t x)))
    (is (= true (:added x)))
    (is (nil? (:nope x)))
    (is (= :dflt (:nope x :dflt)))
    (is (= 1 (get x :e)))))

(deftest indexed-and-seq
  (let [x (d/datom 1 2 "v" 3 false)]
    (is (= [1 2 "v" 3 false] (vec (seq x))))
    (is (= 5 (count x)))
    (is (= 1 (nth x 0)))
    (is (= false (nth x 4)))
    (is (= :nf (nth x 5 :nf)))
    (is (thrown? IndexOutOfBoundsException (nth x 5)))
    (let [[e a v tx added] x]
      (is (= [1 2 "v" 3 false] [e a v tx added])))))

(deftest equality-and-hash
  (let [x (d/datom 1 2 "v" 3 true)
        y (d/datom 1 2 "v" 3 true)]
    (is (= x y))
    (is (= (hash x) (hash y)))
    (is (= (.hashCode ^Object x) (.hashCode ^Object y)))
    (is (= 1 (count (set [x y]))))
    (doseq [other [(d/datom 9 2 "v" 3 true) (d/datom 1 9 "v" 3 true)
                   (d/datom 1 2 "w" 3 true) (d/datom 1 2 "v" 9 true)
                   (d/datom 1 2 "v" 3 false)]]
      (is (not= x other)))
    (is (not= x [1 2 "v" 3 true]))
    (is (not= x nil))))

(deftest printing
  (is (= "#datom[1 2 \"v\" 3 true]" (pr-str (d/datom 1 2 "v" 3 true))))
  (is (= "#datom[1 2 :a/b 3 false]" (pr-str (d/datom 1 2 :a/b 3 false)))))

(deftest value-equality-matches-storage
  (testing "byte arrays compare by content (the documented deviation)"
    (let [x (d/datom 1 2 (byte-array [1 2 3]) 3 true)
          y (d/datom 1 2 (byte-array [1 2 3]) 3 true)]
      (is (= x y))
      (is (= (hash x) (hash y)))
      (is (= (.hashCode ^Object x) (.hashCode ^Object y)))
      (is (= 1 (count (set [x y]))))
      (is (not= x (d/datom 1 2 (byte-array [1 2 4]) 3 true)))))
  (testing "BigDecimals compare exactly (scale is part of the value)"
    (is (= (d/datom 1 2 1.0M 3 true) (d/datom 1 2 1.0M 3 true)))
    (is (not= (d/datom 1 2 1.0M 3 true) (d/datom 1 2 1.00M 3 true)))
    (is (= 2 (count (set [(d/datom 1 2 1.0M 3 true) (d/datom 1 2 1.00M 3 true)]))))))
