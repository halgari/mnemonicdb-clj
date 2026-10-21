(ns mnemonic.reflection-test
  "Loads every src namespace in a fresh JVM (same classpath) and fails on any
  reflection warning. A subprocess avoids :reload, which would redefine
  deftype/definterface classes under already-compiled code."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]))

(defn- src-namespaces []
  (->> (file-seq (io/file "src"))
       (filter #(str/ends-with? (.getName ^java.io.File %) ".clj"))
       (map #(-> (.getPath ^java.io.File %)
                 (str/replace #"^src[/\\]" "")
                 (str/replace #"\.clj$" "")
                 (str/replace #"[/\\]" ".")
                 (str/replace "_" "-")
                 symbol))
       sort))

(deftest no-reflection-warnings
  (let [java (str (io/file (System/getProperty "java.home") "bin" "java"))
        form (pr-str `(binding [*warn-on-reflection* true]
                        (doseq [n# '~(src-namespaces)] (require n#))
                        (shutdown-agents)))
        pb   (doto (ProcessBuilder. ^java.util.List
                                    [java "--enable-native-access=ALL-UNNAMED"
                                     "-cp" (System/getProperty "java.class.path")
                                     "clojure.main" "-e" form])
               (.redirectErrorStream true))
        proc (.start pb)
        out  (slurp (.getInputStream proc))
        exit (.waitFor proc)]
    (is (zero? exit) out)
    (is (not (str/includes? out "Reflection warning")) out)))
