(ns amjil.util.sql
  (:require [amjil.db :as db]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

(defn table-column [name]
  (let [db-result (jdbc/query db/td [(str "help table " name)])
        stringified-cols (map #(get (clojure.walk/stringify-keys %) "column name") db-result)]
    (map #(-> % str/trim str/lower-case) stringified-cols)))

(defn column-string [cols]
  (->> (map #(str (if (= "cal_date" %) "cast(cal_date as char(8))" %) " as " %) cols)
    (str/join ",")))

(defn sql-condition [columns]
  (condp #(contains? (set %2) %1) columns
    "cal_date"    [1 1 "cal_date = ?"]
    "cal_month"   [1 2 "cal_month = ?"]
    "proc_dt"     [1 2 "proc_dt = ?"]
    "proc_date"   [1 2 "proc_date = ?"]
    [0 1 "1=1"]))

(defn gen-sql-from-table
  [name date]
  (let [columns (table-column name)
        [cond-type date-type where-cond] (sql-condition columns)
        selects (column-string columns)
        sql (str "select " selects " from " name " where " where-cond)]
    (condp = cond-type
      1 [sql (condp = date-type
               1 date
               2 (subs date 0 6))]
      0 [sql])))

(defn gen-sql-from-view
  [name date]
  nil)

(defn gen-sql [name type]
  "type => 1 from table 2 from view"
  (condp = type
    "1" (gen-sql-from-table name)
    "2" (gen-sql-from-view name)))

;
; (re-find #"(?<=\()(\s|\w|\r|,)+(?=\))" xx)
;REPLACE VIEW PVIEW.VW_CDE_STREAM\r(\r      STREAM_LEVEL \r      ,STREAM   \r)\rAS LOCKING NMART.CDE_STREAM FOR ACCESS \rSELECT\r      STREAM_LEVEL (TITLE '流量层次'),\r      STREAM
; (TITLE '流量')\r FROM NMART.CDE_STREAM;
