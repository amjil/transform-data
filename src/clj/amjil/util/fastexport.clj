(ns amjil.util.fastexport
  (:require [amjil.db :as db]
            [clojure.java.jdbc :as jdbc]))

(defn- get-connection [d]
  (.getConnection (:datasource d)))

(defn- exec-statment [conn sql]
  (let [stmt (.createStatement conn)
        rs (.executeQuery stmt sql)]
    (drop 1 (jdbc/result-set-seq rs {:as-arrays? true}))))

(defn- unload-to-file [conn file sql]
  (with-open [w (clojure.java.io/writer file :append true)]
    (as-> (exec-statment conn sql) m
          (map #(clojure.string/join #"\t" %) m)
          (doseq [line m]
            (.write w line)
            (.newLine w)))
    (.flush w)))

(defmacro fast-export- [ds file sql]
  `(let [conn# (get-connection ~ds)]
     (unload-to-file conn# ~file ~sql)
     (.close conn#)))

; (defn fast-export [file sql]
;   (fast-export- db/fast file sql))

(defn fast-export [file sql]
  (jdbc/db-query-with-resultset db/fast sql
    (fn [rs]
      (with-open [w (clojure.java.io/writer file :append true)]
        (as-> (drop 1 (jdbc/result-set-seq rs {:as-arrays? true})) m
              (map #(clojure.string/join #"\t" %) m)
              (doseq [line m]
                (.write w line)
                (.newLine w)))
        (.flush w)))))
