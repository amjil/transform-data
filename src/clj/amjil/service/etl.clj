(ns amjil.service.etl
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [amjil.db :as db]
            [amjil.config :refer [env]]
            [amjil.util.etl :as etl]
            [clj-time.format :as time-format]))


(defn transformx [date]
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
                      table-in)
        data (jdbc/query db/tdcore [sql-str date])]
    (if (empty? data)
      (log/warn "The ETL  date = " date " has not done yet.......")
      (doall
        (for [d data]
          (let [job-done (jdbc/query db/sqlite ["select * from job_logs where job_date = ? and job_nm = ?" (:job_date d) (:job_nm d)])
                date (str/replace (:job_date d) #"-" "")
                table (str/lower-case (str (:dbname d) "." (:job_nm d)))]
            (log/warn d)
            (if (empty? job-done)
              (do
                (etl/etl-transaction date table)
                (let [filename (str "./data/" date "/" table ".txt")
                      file (io/as-file filename)]
                  (if (.exists file)
                    (jdbc/insert! db/sqlite :job_logs (select-keys d [:job_nm :job_date :batch_id]))
                    (log/error "File is zero content!!!")))))))))))
