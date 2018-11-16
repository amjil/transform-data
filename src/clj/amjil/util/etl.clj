(ns amjil.util.etl
  (:require [amjil.util.export :as export]
            [amjil.util.fastexport :as fast]
            [amjil.util.import :as im]
            [amjil.util.sql :as sql]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]))

(defn etl-transaction [date name]
  (let [[out outtype in] (sql/table-name name)
        sql (sql/gen-sql out date)
        length-of-sql (count sql)
        im-type (if (= 1 length-of-sql) 0 1)
        date (if (= 1 length-of-sql) date (last sql))
        filename (str "./data/" date "/" name ".txt")]
    (clojure.java.io/make-parents filename)
    (if (= 0 outtype)
      (export/unload-to-file filename sql)
      (fast/fast-export filename sql))
    (let [file (io/as-file filename)]
      (if (and (.exists file) (< 0 (.length file)))
        (im/import im-type filename date name)
        (log/error "File is zero content!!!")))))


(defn etl-export [date name]
  (let [[out outtype in] (sql/table-name name)
        sql (sql/gen-sql out date)
        length-of-sql (count sql)
        im-type (if (= 1 length-of-sql) 0 1)
        date (if (= 1 length-of-sql) date (last sql))
        filename (str "./data/" date "/" name ".txt")]
    (clojure.java.io/make-parents filename)
    (if (= 0 outtype)
      (export/unload-to-file filename sql)
      (fast/fast-export filename sql))
    (let [file (io/as-file filename)]
      (if-not (and (.exists file) (< 0 (.length file)))
        (log/error "File is zero content!!!")))))
