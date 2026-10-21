(ns mnemonic.segcache-heap-test
  "The segment cache's byte accounting follows the real heap across value types (the
  budget is only meaningful if it does), and values handed out from shared cached
  Datoms cannot be mutated into later reads."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.segcache :as sc])
  (:import (java.util Date UUID)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- used ^long []
  (dotimes [_ 4] (System/gc) (Thread/sleep 100))
  (let [r (Runtime/getRuntime)] (- (.totalMemory r) (.freeMemory r))))

(defn- ratio
  "Measured heap / estimated bytes for every [e] segment of `n` entities with 4
  attributes of type `vt` (values from `vf`), all Datoms decoded."
  [n vt vf]
  (let [uri (str "mnemonic:mem://heap-" (System/nanoTime) "?index-cache-mb=1024")]
    (m/create-database uri)
    (let [c (m/connect uri)
          attrs [:r/a :r/b :r/c :r/d]]
      @(m/transact c (for [a attrs] {:db/ident a :db/valueType vt :db/cardinality :db.cardinality/one}))
      (let [es (vec (mapcat (fn [chunk]
                              (vals (:tempids @(m/transact c (for [i chunk]
                                                               (into {:db/id (str i)}
                                                                     (map-indexed (fn [j a] [a (vf (+ i j))]) attrs)))))))
                            (partition-all 5000 (range n))))
            db (m/db c)
            cache (:segcache (:store c))]
        (sc/clear! cache)
        (let [u0 (used)]
          ;; separate reads of each entity (admission), then every Datom decoded
          (dotimes [_ 2] (doseq [e es] (dorun (m/datoms db :eavt e))))
          (doseq [e es] (dorun (map :v (m/datoms db :eavt e))))
          (let [u1 (used)
                est (:bytes (m/index-cache-stats c))
                r (/ (double (- u1 u0)) (double est))]
            (is (= (count es) (:entries (m/index-cache-stats c))))
            (m/release c)
            r))))))

(deftest weight-tracks-heap-test
  (doseq [[label vt vf] [["longs" :db.type/long long]
                         ["strings 20" :db.type/string #(format "value-%014d" %)]
                         ["strings 200" :db.type/string #(apply str (format "value-%014d" %) (repeat 180 "x"))]
                         ["instants" :db.type/instant #(Date. (long %))]
                         ["uuids" :db.type/uuid #(UUID. 0 (long %))]
                         ["keywords" :db.type/keyword #(keyword "k" (str "v" (mod % 50)))]
                         ["bytes 16" :db.type/bytes #(.getBytes (format "%016d" %))]]]
    (let [r (ratio 20000 vt vf)]
      (println "segcache heap/estimate" label (format "%.2f" r))
      ;; the estimate targets +-15%; the bound leaves room for GC measurement noise
      (is (< 0.8 r 1.25) (str label " heap/estimate " r)))))

(deftest cached-values-are-not-shared-test
  (let [uri (str "mnemonic:mem://heap-mut-" (System/nanoTime) "?index-cache-mb=64")]
    (m/create-database uri)
    (let [c (m/connect uri)
          _ @(m/transact c [{:db/ident :q/i :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
                            {:db/ident :q/b :db/valueType :db.type/bytes :db/cardinality :db.cardinality/one}
                            {:db/ident :q/t :db/valueType :db.type/tuple :db/tupleTypes [:db.type/instant :db.type/long]
                             :db/cardinality :db.cardinality/one}])
          e (get (:tempids @(m/transact c [{:db/id "e" :q/i (Date. 1000) :q/b (byte-array [1 2 3])
                                            :q/t [(Date. 5) 7]}])) "e")
          db (m/db c)
          v (fn [a] (:v (first (m/datoms db :eavt e a))))]
      (dotimes [_ 12] (v :q/i) (v :q/b) (v :q/t) (m/pull db '[*] e))
      (is (pos? (:hits (m/index-cache-stats c))))
      (.setTime ^Date (v :q/i) 999999)
      (aset ^bytes (v :q/b) 0 (byte 99))
      (.setTime ^Date (first (v :q/t)) 999999)
      (.setTime ^Date (:q/i (m/entity db e)) 999999)
      (.setTime ^Date (:q/i (m/pull db '[:q/i] e)) 999999)
      (is (= (Date. 1000) (v :q/i)))
      (is (= [1 2 3] (vec (v :q/b))))
      (is (= [(Date. 5) 7] (v :q/t)))
      (is (= (Date. 1000) (:q/i (m/entity db e))))
      (is (= (Date. 1000) (:q/i (m/pull db '[:q/i] e)))))))
