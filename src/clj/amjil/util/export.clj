(ns amjil.util.export)

(defn unload-to-file [date file sql]
  (let [data (drop 1 (jdbc/query db/td sql {:as-arrays? true}))
        filename (str "./data/" date "/" file ".txt")]
    (clojure.java.io/make-parents (str "./data/" date "/a.txt"))
    (spit filename "")
    (as-> (map #(str/join "\t" %) data) m
      (str/join "\r\n" m)
      (str/replace m #"\\" "")
      (spit filename m :append true))))
