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
  "return type : 1st has partition 2nd date or month"
  (condp #(contains? (set %2) %1) columns
    "cal_date"    [1 1 "cal_date = ?"]
    "cal_month"   [1 2 "cal_month = ?"]
    "proc_dt"     [1 2 "proc_dt = ?"]
    "proc_date"   [1 2 "proc_date = ?"]
                  [0 0 "1=1"]))

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
  [name date]
  (let [type (str/starts-with? (str/lower-case name) "pview.")
        columns (if type
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

(defn table-name [name]
  (let [table-conf (get (:table-conf env) name)]
    (if (empty? table-conf)
      [name name]
      (let [out (get table-conf :unload)
            outtype (get table-conf :otype)
            in  (get table-conf :load)]
        [(if (empty? out) name out)
         otype
         (if (empty? in)  name in)
         itype]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn query-sqlite-job-log [job]
  (let [date (:job_date job)
        job-name (:job-nm job)]
    (jdbc/query db/sqlite ["select * from job_logs where job_date = ? and job_nm = ?" date job-name])))

(defn query-td-job-log [date]
  (let [tables (:tables env)
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
