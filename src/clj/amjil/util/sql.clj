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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn view-columns [name]
  (let [view-def (-> (jdbc/query db/td [(str "show view " name)])
                     first
                     (get (keyword "request text"))
                     str/lower-case)
        view-find-cols (first (re-find #"(?<=\()(\s|\w|\r|,)+(?=\)\s+as)" view-def))]
    (-> (str/replace view-find-cols #"\r+|\s+" "")
        (str/split #","))))

(defn gen-sql
  [type name date]
  (let [columns (if (= 1 type)
                  (table-column name)
                  (view-columns name))
        [cond-type date-type where-cond] (sql-condition columns)
        selects (column-string columns)
        sql (str "select " selects " from " name " where " where-cond)]
    (condp = cond-type
      1 [sql (condp = date-type
               1 date
               2 (subs date 0 6))]
      0 [sql])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn query-td-job-log [date]
  (let [tables (concat
                 (-> (slurp "withdays.txt") (str/split #"\r\n"))
                 (-> (slurp "nodays.txt") (str/split #"\r\n")))
        tables (map #(last (str/split % #"\.")) tables)
        table-in (str "('" (str/join "','" tables) "')")
        sql-str (str "SEL a.TXDate as job_date ,a.JOB_BATCH   as  batch_id
                                      , a.job_nm as job_nm, trim(b.DatabaseName) as dbname
                                      FROM  PVIEW.VW_DSYNC_JOB_DONE a LEFT JOIN DBC.TABLES b
                                      ON a.JOB_NM=b.TABLENAME
                                      where SYNC_DT= ?
                                      and trim(b.DatabaseName) in ('pdata', 'pmart', 'nmart', 'BASS1','DBODATA','KDDMART')
                                      and a.JOB_NM in "
                      table-in)]
    (jdbc/query db/tdcore [sql-str date])))

(defn query-sqlite-job-log [date]
  (jdbc/query db/sqlite ["select * from job_logs where job_date = ?" date]))

(defn job-prepare-sync [date]
  (let [td-jobs (query-td-job-log date)
        done-jobs (query-sqlite-job-log date)
        prepared-jobs (clojure.set/difference (set (map #(:job_nm %) td-jobs))
                                              (set (map #(:job_nm %) done-jobs)))
        sync-jobs (filter #(contains? prepared-jobs (:job_nm %) td-jobs))]
    (doall
      nil)))
