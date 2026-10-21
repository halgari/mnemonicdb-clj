(ns mnemonic.r25-startup
  "Start-to-first-answer breakdown. Run in a fresh JVM against a built db:
  clojure -M -m mnemonic.r25-startup <dir>   (classpath: src + bench)"
  )

(defn -main [dir]
  (let [jvm-ms (- (System/currentTimeMillis) (.getStartTime (java.lang.management.ManagementFactory/getRuntimeMXBean)))
        t0 (System/nanoTime)
        _ (require 'mnemonic.api)
        t1 (System/nanoTime)
        m-connect (resolve 'mnemonic.api/connect)
        m-db (resolve 'mnemonic.api/db)
        m-entity (resolve 'mnemonic.api/entity)
        m-q (resolve 'mnemonic.api/q)
        conn (m-connect (str "mnemonic:file://" dir))
        t2 (System/nanoTime)
        db (m-db conn)
        e (m-entity db [:p/email "user5@example.com"])
        age (:p/age e)
        t3 (System/nanoTime)
        r (m-q '[:find ?e :where [?e :p/email "user7@example.com"]] db)
        t4 (System/nanoTime)
        ms #(format "%.0f" (/ (double (- %2 %1)) 1e6))]
    (println "jvm boot to main ms" jvm-ms
             "| require ms" (ms t0 t1) "| connect ms" (ms t1 t2)
             "| first entity ms" (ms t2 t3) "| first query ms" (ms t3 t4)
             "| total in main ms" (ms t0 t4))
    ((resolve 'mnemonic.api/release) conn)
    (shutdown-agents)))
