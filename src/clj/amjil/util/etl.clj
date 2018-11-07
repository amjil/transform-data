(ns amjil.util.etl
  (:require [amjil.export :as export]
            [amjil.fast :as fast]
            [amjil.import :as im]
            [amjil.sql :as sql]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]))

(defn etl-transaction [date name]
  (let [[out outtype in] (sql/table-name name)
        sql (sql/gen-sql out date)
        filename (str "./data/" date "/" name ".txt")
        im-type (if (= 1 (count sql)) 0 1)]
    (if (= 1 outtype)
      (fast/fast-export filename sql)
      (export/unload-to-file filename sql))
    (let [file (io/as-file filename)]
      (if (and (.exists file) (< 0 (.length file)))
        (im/import im-type filename name)
        (log/error "File is zero content!!!")))))
