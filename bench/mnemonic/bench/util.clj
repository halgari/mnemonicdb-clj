(ns mnemonic.bench.util
  "Env-driven config, timing helpers and statistics for the benchmark suite."
  (:require [clojure.string :as str]))

(defn env [k default]
  (or (System/getenv k) default))

(def scale
  (Double/parseDouble (env "MNEMONIC_BENCH_SCALE" "1.0")))

(defn size
  "Workload size: env MNEMONIC_BENCH_<NAME> (absolute) or default * MNEMONIC_BENCH_SCALE."
  [env-name default]
  (if-let [v (System/getenv (str "MNEMONIC_BENCH_" env-name))]
    (Long/parseLong v)
    (max 1 (long (Math/round (* scale (double default)))))))

(defn now-ns ^long [] (System/nanoTime))

(defmacro timed-ns
  "[result elapsed-ns]"
  [& body]
  `(let [t0# (System/nanoTime)
         r# (do ~@body)]
     [r# (- (System/nanoTime) t0#)]))

(defn gc! []
  (System/gc)
  (Thread/sleep 100))

(defn percentile
  "p in [0,1] over a sorted vector."
  [sorted p]
  (let [n (count sorted)]
    (nth sorted (min (dec n) (long (Math/floor (* p n)))))))

(defn run-for
  "Calls (f) repeatedly: first `warm-ms` of warmup (at least `min-iters` calls), then
  measures for `run-ms` (at least `min-iters` calls). Returns
  {:iters n :mean-ms :p50-ms :p99-ms :min-ms :result <last result>}."
  [f {:keys [warm-ms run-ms min-iters] :or {warm-ms 1500 run-ms 3000 min-iters 5}}]
  (let [warm-end (+ (now-ns) (* (long warm-ms) 1000000))]
    (loop [i 0]
      (f)
      (when (or (< i (dec (long min-iters))) (< (now-ns) warm-end))
        (recur (inc i)))))
  (gc!)
  (let [end (+ (now-ns) (* (long run-ms) 1000000))
        times (java.util.ArrayList.)
        last-r (volatile! nil)]
    (loop [i 0]
      (let [t0 (now-ns)
            r (f)
            dt (- (now-ns) t0)]
        (.add times dt)
        (vreset! last-r r)
        (when (or (< i (dec (long min-iters))) (< (now-ns) end))
          (recur (inc i)))))
    (let [sorted (vec (sort (vec times)))
          n (count sorted)
          ms #(/ (double %) 1e6)]
      {:iters n
       :mean-ms (ms (/ (reduce + sorted) (double n)))
       :p50-ms (ms (percentile sorted 0.5))
       :p99-ms (ms (percentile sorted 0.99))
       :min-ms (ms (first sorted))
       :result @last-r})))

(defn fmt-num [x]
  (cond
    (nil? x) "n/a"
    (integer? x) (format "%,d" x)
    (>= (Math/abs (double x)) 1000) (format "%,.0f" (double x))
    (>= (Math/abs (double x)) 100) (format "%.0f" (double x))
    (>= (Math/abs (double x)) 10) (format "%.1f" (double x))
    (>= (Math/abs (double x)) 1) (format "%.2f" (double x))
    :else (format "%.3f" (double x))))

(defn sh
  "Runs a command, returns trimmed stdout or nil."
  [& args]
  (try
    (let [p (-> (ProcessBuilder. ^java.util.List (vec args)) (.redirectErrorStream true) .start)
          out (slurp (.getInputStream p))]
      (.waitFor p)
      (str/trim out))
    (catch Throwable _ nil)))
