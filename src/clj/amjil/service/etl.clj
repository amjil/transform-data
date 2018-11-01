(ns amjil.service.etl
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [amjil.db :as db]
            [amjil.config :refer [env]])
  (:use [amjil.service.transform]))

(defn export-table [type table-name date]
  (let [columns (table-column table-name)
        column-str (column-string table-name)
        flag (not= -1 (.indexOf columns "cal_date"))
        proc-dt-flag (not= -1 (.indexOf columns "proc_dt"))
        proc-date-flag (not= -1 (.indexOf columns "proc_date"))
        mon-flag (not= -1 (.indexOf columns "cal_month"))
        tname (last (str/split table-name #"\."))
        tname (cond
                (contains? (set (:pview env)) tname) (str "pview.vw_" tname)
                (true? ((:trans env) tname)) ((:trans env) tname)
                :else table-name)
        sql-str (str "select " column-str " from " tname
                  (if flag " where cal_date = ?" " where 1=1") (if mon-flag " and cal_month = ?")
                  (if proc-dt-flag " and proc_dt = ?")
                  (if proc-date-flag  " and proc_date = ?"))
        filename (str "./data/" date "/" table-name ".txt")
        db-spec (if (= "1" type) db/tdcore db/td)]
    (log/warn "Download table from " (if (= "1" type) " Core " " Front "))
    (log/warn "Download date = " date)
    (log/warn "The Sql Statment = " sql-str)
    (log/warn "The table name = " table-name " is starting.....")
    (clojure.java.io/make-parents (str "./data/" date "/a.txt"))
    (spit filename "")
    (let [data (drop 1 (jdbc/query db-spec (if (or flag mon-flag) [sql-str date] [sql-str]) {:as-arrays? true}))]
      (as-> (map #(str/join "\t" %) data) m
        (str/join "\r\n" m)
        (str/replace m #"\\" "")
        (spit filename m :append true)))
    (log/warn "The table name = " table-name " is complated.....")))

(defn export-big [table-name date]
  (let [filename (str "./data/" date "/" table-name ".txt")
        column-str (column-string table-name)
        sql-str (str "select " column-str " from " table-name " where cal_date = ?  and lac_id in (select lac_id from pmart.mid_scenery_hotel_any_info where city_id = ?)")]
    (log/warn "The table name = " table-name " is starting.....")
    (spit filename "")
    (doall
      (for [city citys]
        (let [data (drop 1 (jdbc/query db/td [sql-str date city] {:as-arrays? true}))]
          (as-> (map #(str/join "\t" %) data) m
            (str/join "\r\n" m)
            (spit filename m :append true)))))
    (let [sql-str (str "select " column-str " from " table-name " where cal_date = ? and lac_id not in (select lac_id from pmart.mid_scenery_hotel_any_info)")
          data (drop 1 (jdbc/query db/td [sql-str date] {:as-arrays? true}))]
      (as-> (map #(str/join "\t" %) data) m
        (str/join "\r\n" m)
        (spit filename m :append true)))
    (log/warn "The table name = " table-name " is starting.....")))

(defn import-data [type date]
  (let [runtime (Runtime/getRuntime)
        process (case type
                  1 (.exec runtime (str "sh load-noday.sh " date))
                  2 (.exec runtime (str "sh delete.sh " date))
                  3 (.exec runtime (str "sh load.sh " date))
                  4 (.exec runtime (str "sh load-big.sh " date)))]
    (.waitFor process)
    (log/warn "The import type = " type)
    (log/warn (-> (.getInputStream process) (io/input-stream) slurp))))

(defn import-url [dir type table date]
  (let [runtime (Runtime/getRuntime)
        process (if (= "1" type)
                  (.exec runtime (str "sh urlload.sh " dir " " date " " table))
                  (.exec runtime (str "sh urlnoday.sh " dir " " table)))]
    (.waitFor process)
    (log/warn "result = " (-> (.getInputStream process) (io/input-stream) slurp))
    (log/warn "table " table " import success")))

(defn import-datax [type table date]
  (let [runtime (Runtime/getRuntime)
        table-name (cond
                     (contains? (set (:kmart env)) table) (str/replace table #"nmart" "kmart")
                     (contains? (set (:bigdatamap env)) table) (str/replace table #"nmart" "bigdatamap")
                     :else table)
        process (cond
                  (= "1" type) (.exec runtime (str "sh load.sh " date " " table " " table-name))
                  (= "2" type) (.exec runtime (str "sh noday.sh " date " " table " " table-name))
                  :else (.exec runtime (str "sh mon.sh " date " " table " " table-name)))]
    (.waitFor process)
    (log/warn "result = " (-> (.getInputStream process) (io/input-stream) slurp))
    (log/warn "table " table " import success")))

(defn import [date]
  (let [tables (-> (slurp "withdays.txt") (str/split #"\r\n"))]
    (doall (map #(import-datax "1" % date) tables)))
  (let [tables (-> (slurp "nodays.txt") (str/split #"\r\n"))]
    (doall (map #(import-datax "2" % date) tables)))
  (import-datax "1" "nmart.rpt_band_area_yx_daily" date)
  (import-datax "1" "pmart.mid_scenery_arry_in_ter_day" date))

(defn export [date]
  (clojure.java.io/make-parents (str "./data/" date "/a.txt"))
  (let [tables (-> (slurp "withdays.txt") (str/split #"\r\n"))]
    (doall (map #(export-table "2" % date) tables)))
  (let [tables (-> (slurp "nodays.txt") (str/split #"\r\n"))]
    (doall (map #(export-table "2" % date) tables)))
  (export-big "nmart.rpt_band_area_yx_daily" date)
  (export-big "pmart.mid_scenery_arry_in_ter_day" date))

(defn transformx [date]
  (let [;job-data (jdbc/query db/sqlite ["select * from job_logs where job_date = ?" date])
        tables (concat
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
                (jdbc/insert! db/sqlite :job_logs (select-keys d [:job_nm :job_date :batch_id]))
                (export-table "2" table date)
                (let [cols (table-column table)
                      flag (not= -1 (.indexOf cols "cal_date"))]
                  (log/warn "import table's type = " (if flag 1 2))
                  (import-datax (if flag "1" "2") table date))))))))))
