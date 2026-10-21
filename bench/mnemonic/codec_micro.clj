(ns mnemonic.codec-micro
  "Encode/decode microbenchmark: current codec vs the frozen original (test/mnemonic/codec_old.clj).
  clojure -A:test:bench -M -m mnemonic.codec-micro   (needs the test path for codec-old)"
  (:require [mnemonic.codec :as c]
            [mnemonic.codec-old :as old]))

(set! *warn-on-reflection* true)

(defmacro ^:private bench [label n & body]
  `(do (dotimes [_# 3] (dotimes [_# ~n] ~@body))
       (let [t0# (System/nanoTime)]
         (dotimes [_# 5] (dotimes [_# ~n] ~@body))
         (printf "%-34s %8.1f ns/op%n" ~label (/ (double (- (System/nanoTime) t0#)) (* 5.0 ~n))))))

(defn -main [& _]
  (let [n 200000
        sk (c/encode-key :eavt 17592186045418 100 "alice@example.com" 13194139533313)
        lk (c/encode-key :aevt 100 17592186045418 42 13194139533313)
        dat (c/decode-key :eavt sk)
        datoms (vec (repeat 6 dat))
        le (c/encode-log-entry datoms)]
    (println "-- encode")
    (bench "new encode-key aevt long" n (c/encode-key :aevt 100 17592186045418 42 13194139533313))
    (bench "old encode-key aevt long" n (old/encode-key :aevt 100 17592186045418 42 13194139533313))
    (bench "new encode-key eavt string" n (c/encode-key :eavt 17592186045418 100 "alice@example.com" 13194139533313))
    (bench "old encode-key eavt string" n (old/encode-key :eavt 17592186045418 100 "alice@example.com" 13194139533313))
    (bench "new encode-prefix aevt [a]" n (c/encode-prefix :aevt [100]))
    (bench "old encode-prefix aevt [a]" n (old/encode-prefix :aevt [100]))
    (bench "new value->bytes kw" n (c/value->bytes :person/name))
    (bench "old value->bytes kw" n (old/value->bytes :person/name))
    (bench "new encode-log-entry x6" (quot n 4) (c/encode-log-entry datoms))
    (bench "old encode-log-entry x6" (quot n 4) (old/encode-log-entry datoms))
    (println "-- decode")
    (bench "new decode-key aevt long" n (c/decode-key :aevt lk))
    (bench "old decode-key aevt long" n (old/decode-key :aevt lk))
    (bench "new decode-key eavt string" n (c/decode-key :eavt sk))
    (bench "old decode-key eavt string" n (old/decode-key :eavt sk))
    (bench "new decode-log-entry x6" (quot n 4) (c/decode-log-entry 5 le))
    (bench "old decode-log-entry x6" (quot n 4) (old/decode-log-entry 5 le))
    (println "-- datom hash")
    (bench "datom hash" n (hash dat))))
