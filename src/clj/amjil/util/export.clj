(ns amjil.util.export
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [amjil.db :as db]))

(defn unload-to-file [file sql]
  (let [data (drop 1 (jdbc/query db/td sql {:as-arrays? true}))
        filename file]
    (clojure.java.io/make-parents filename)
    (spit filename "")
    (as-> (map #(str/replace (str/join "\t" %) #"\\" "") data) m
      (str/join "\r\n" m)
      (spit filename m :append true))))
