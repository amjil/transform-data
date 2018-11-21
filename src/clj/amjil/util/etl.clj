(ns amjil.util.etl
  (:require [amjil.util.export :as export]
            [amjil.util.fastexport :as fast]
            [amjil.util.import :as im]
            [amjil.util.sql :as sql]
            [amjil.config :refer [env]]
            [amjil.db :as db]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]))

(defn import-type [name value]
  (let [imtype (-> (get (:table-conf env) name) (get :itype))]
    (if (empty? imtype)
      value
      imtype)))

(defn etl-params [date name]
  (let [[out outtype in db-type] (sql/table-name name)
        dbconn (condp = db-type
                 0 db/td
                 1 db/fast
                 2 db/tdcore)
        sql (sql/gen-sql dbconn out date)
        length-of-sql (count sql)
        im-type (import-type name (if (= 1 length-of-sql) 0 1))
        date (if (= 1 length-of-sql) date (last sql))
        filename (str "./data/" date "/" name ".txt")]
    (clojure.java.io/make-parents filename)
    [outtype im-type filename date dbconn sql]))

(defn etl-transaction [date name]
  (let [[outtype im-type filename date dbconn sql] (etl-params date name)]
    (if (= 0 outtype)
      (export/unload-to-file dbconn filename sql)
      (fast/fast-export dbconn filename sql))
    (let [file (io/as-file filename)]
      (if (and (.exists file) (< 0 (.length file)))
        (im/import im-type filename date name)
        (log/error "File is zero content!!!")))))

(defn etl-export [date name]
  (let [[outtype im-type filename date dbconn sql] (etl-params date name)]
    (if (= 0 outtype)
      (export/unload-to-file dbconn filename sql)
      (fast/fast-export dbconn filename sql))
    (let [file (io/as-file filename)]
      (if-not (and (.exists file) (< 0 (.length file)))
        (log/error "File is zero content!!!")))))

(defn etl-import [date name]
  (let [[outtype im-type filename date dbconn sql] (etl-params date name)]
    (let [file (io/as-file filename)]
      (if (and (.exists file) (< 0 (.length file)))
        (im/import im-type filename date name)
        (log/error "File is zero content!!!")))))
