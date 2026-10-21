(ns mnemonic.prep-bench
  "Microbenchmark of tx/prepare (no writes): clojure -M:bench -m mnemonic.prep-bench"
  (:require [mnemonic.api :as m]
            [mnemonic.tx :as tx]
            [mnemonic.bench.data :as data]))

(defn- state [conn] @(:state conn))

(defn- time-prepare [label f iters]
  (dotimes [_ 5] (f))
  (System/gc)
  (let [ts (vec (sort (repeatedly iters #(let [s (System/nanoTime)] (f) (- (System/nanoTime) s)))))]
    (println (format "%-28s p50 %.2f ms (%.1f us)  min %.2f ms" label
                     (/ (nth ts (quot (count ts) 2)) 1e6) (/ (nth ts (quot (count ts) 2)) 1e3) (/ (first ts) 1e6)))))

(defn -main [& args]
  (let [uri "mnemonic:mem://prep-bench"
        _ (m/create-database uri)
        conn (m/connect uri)]
    @(m/transact conn data/schema)
    @(m/transact conn data/enum-idents)
    (doseq [from (range 0 20000 1000)]
      @(m/transact conn (data/people 1 from (+ from 1000))))
    (let [{:keys [db meta]} (state conn)
          fresh (data/people 1 100000 101000)
          upd (mapv (fn [i] [:db/add [:p/email (data/email i)] :p/age 5000]) (range 0 1000))
          upsert (mapv (fn [i] (assoc (data/person 1 i) :p/age 4242)) (range 1000 2000))]
      (when (= "small" (first args))
        (let [stx (mapv data/small-tx (range 100000 102000))]
          (dotimes [i 600000] (tx/prepare db meta (nth stx (rem i 2000))))))
      (when (= "profile" (first args))
        (dotimes [_ 400] (tx/prepare db meta fresh)))
      (time-prepare "prepare 1000 new people" #(tx/prepare db meta fresh) 30)
      (time-prepare "prepare 1000 lookup-ref upd" #(tx/prepare db meta upd) 30)
      (time-prepare "prepare 1000 upserts" #(tx/prepare db meta upsert) 30)
      (let [stx (mapv data/small-tx (range 100000 102000))
            i (volatile! -1)]
        (time-prepare "prepare small-tx shape" #(tx/prepare db meta (nth stx (rem (vswap! i inc) 2000))) 4000))
      (time-prepare "prepare small (1 person)" #(tx/prepare db meta [(first fresh)]) 2000))
    (m/release conn)
    (m/delete-database uri)
    (shutdown-agents)))
