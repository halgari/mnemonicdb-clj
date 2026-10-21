(ns mnemonic.lifecycle-test
  "Runs mnemonic.lifecycle-scenarios in a fresh JVM: these scenarios used to crash the
  JVM (SIGSEGV in RocksDB) and must never take down the test runner."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]))

(deftest lifecycle-scenarios
  (let [java (str (io/file (System/getProperty "java.home") "bin" "java"))
        err-dir (System/getProperty "java.io.tmpdir")
        pb (doto (ProcessBuilder. ^java.util.List
                                  [java "--enable-native-access=ALL-UNNAMED"
                                   (str "-XX:ErrorFile=" err-dir "/mnemonic_hs_err_pid%p.log")
                                   "-cp" (System/getProperty "java.class.path")
                                   "clojure.main" "-m" "mnemonic.lifecycle-scenarios"])
             (.redirectErrorStream true))
        proc (.start pb)
        out (slurp (.getInputStream proc))
        exit (.waitFor proc)
        lines (str/split-lines out)
        started (keep #(second (re-matches #"START (\S+)" %)) lines)
        ok (set (keep #(second (re-matches #"SCENARIO (\S+) OK" %)) lines))]
    (is (zero? exit) out)
    (is (seq started) out)
    (doseq [s started]
      (is (contains? ok s) (str "scenario " s " failed or crashed:\n" out)))))
