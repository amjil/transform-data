(ns amjil.service.etl
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [amjil.db :as db])
  (:use [amjil.service.transform]))

(defn export-table [type table-name date]
  (let [columns (table-column table-name)
        column-str (column-string table-name)
        flag (not= -1 (.indexOf columns "cal_date"))
        sql-str (str "select " column-str " from " table-name (if flag " where cal_date = ?" " where 1=1"))
        filename (str "./" date "/" table-name ".txt")
        db-spec (if (= "1" type) db/tdcore db/td)]
    (log/warn "Download table from " (if (= "1" type) " Core " " Front "))
    (log/warn "Download date = " date)
    (log/warn "The table name = " table-name " is starting.....")
    (spit filename "")
    (let [data (drop 1 (jdbc/query db-spec (if flag [sql-str date] [sql-str]) {:as-arrays? true}))]
      (as-> (map #(str/join "\t" %) data) m
        (str/join "\r\n" m)
        (spit filename m :append true)))
    (log/warn "The table name = " table-name " is complated.....")))

(defn export-data [table-name date]
  (let [columns (table-column table-name)
        column-str (column-string table-name)
        flag (not= -1 (.indexOf columns "cal_date"))
        sql-str (str "select " column-str " from " table-name (if flag " where cal_date = ?" " where 1=1"))
        filename (str "./" date "/" table-name ".txt")]
    (log/warn "The table name = " table-name " is starting.....")
    (clojure.java.io/make-parents (str "./" date "/a.txt"))
    (spit filename "")
    (let [data (drop 1 (jdbc/query db/td (if flag [sql-str date] [sql-str]) {:as-arrays? true}))]
      (as-> (map #(str/join "\t" %) data) m
        (str/join "\r\n" m)
        (str/replace m #"\\" "")
        (spit filename m :append true)))
    (log/warn "The table name = " table-name " is complated.....")))

(defn export-big [table-name date]
  (let [filename (str "./" date "/" table-name ".txt")
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

(defn import-datax [type table date]
  (let [runtime (Runtime/getRuntime)
        process (if (= "1" type)
                  (.exec runtime (str "sh load.sh " date " " table))
                  (.exec runtime (str "sh noday.sh " date " " table)))]
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
  (clojure.java.io/make-parents (str "./" date "/a.txt"))
  (let [tables (-> (slurp "withdays.txt") (str/split #"\r\n"))]
    (doall (map #(export-data % date) tables)))
  (let [tables (-> (slurp "nodays.txt") (str/split #"\r\n"))]
    (doall (map #(export-data % date) tables)))
  (export-big "nmart.rpt_band_area_yx_daily" date)
  (export-big "pmart.mid_scenery_arry_in_ter_day" date))

(defn transformx [date]
  (let [;job-data (jdbc/query db/sqlite ["select * from job_logs where job_date = ?" date])
        data (jdbc/query db/tdcore ["SEL a.TXDate as job_date ,a.JOB_BATCH   as  batch_id
                                      , a.job_nm as job_nm, trim(b.DatabaseName) as dbname
                                      FROM  PVIEW.VW_DSYNC_JOB_DONE a LEFT JOIN DBC.TABLES b
                                      ON a.JOB_NM=b.TABLENAME
                                      where SYNC_DT= ?
                                      and trim(b.DatabaseName) in ('pdata', 'pmart', 'nmart')
                                      and a.JOB_NM in
                                      ('rpt_stay_leave_day','rpt_nm_come_day','rpt_outside_come_day','rpt_stay_analy_day','rpt_stay_arry_day','rpt_user_design_anay_day','rpt_scenery_uer_lev_day','rpt_nm_come_line_daily','rpt_scenery_hotel_any_day','rpt_scenery_net_prefer_day','rpt_big_als_track_day','rpt_nm_come_gprs_lev_daily','rpt_nm_come_gsm_lev_daily','rpt_outer_user_daily','rpt_travel_timeing_daily','rpt_site_long_wdaily','rpt_site_long_mdaily','rpt_nm_traveler_daily','rpt_traveler_od_daily','rpt_roma_uer_lev_day','rpt_roma_net_prefer_day','rpt_nm_come_trade_daily','rpt_nm_out_prov_daily','rpt_nm_out_trade_daily','rpt_nm_out_site_long_wdaily','rpt_nm_out_site_long_mdaily','rpt_outside_uer_lev_wdaily','rpt_outside_net_prefer_day','rpt_outside_uer_lev_mdaily','tb_nm_tour','rpt_tra_route_result_day','rpt_scenery_source_day','rpt_scenery_sour_f_day','rpt_trval_info_daily','rpt_user_trval_info_daily','rpt_bigdata_fml_info_daily','rpt_bigdata_fml_bts_day','rpt_scen_hour_subs_daily','rpt_come_user_info_stat','rpt_nm_out_info_daily1','rpt_nm_out_info_daily2','rpt_nm_out_info_daily3','rpt_scen_arry_in_ter_day1','rpt_scen_arry_in_ter_day2','rpt_scen_arry_in_ter_day3','rpt_scen_arry_in_ter_day7','rpt_scen_arry_in_ter_day8','rpt_scen_arry_in_ter_day9',
                                      'tb_res_nwm_cell_his','rpt_long_stay_user_day','rpt_people_strream_daily1','rpt_people_strream_daily4',
                                      'rpt_band_area_yx_daily','mid_scenery_arry_in_ter_day', 'rpt_band_area_yx_daily', 'mid_scenery_arry_in_ter_day')
                                      " date])]
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
                (export-data table date)
                (let [cols (table-column table)
                      flag (not= -1 (.indexOf cols "cal_date"))]
                  (log/warn "import table's type = " (if flag 1 2))
                  (import-datax (if flag "1" "2") table date))))))))))
