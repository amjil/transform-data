(ns amjil.util.export)

(defn unload-to-file [file sql]
  (let [data (drop 1 (jdbc/query db/td sql {:as-arrays? true}))
        filename file]
    (clojure.java.io/make-parents filename)
    (spit filename "")
    (as-> (map #(str/join "\t" %) data) m
      (str/join "\r\n" m)
      (str/replace m #"\\" "")
      (spit filename m :append true))))
