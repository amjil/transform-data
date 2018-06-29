(ns amjil.job.job
  (:require [clojurewerkz.quartzite.jobs :as j :refer [defjob]]
            [clojurewerkz.quartzite.conversion :as qc]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [amjil.db :as db]
            [amjil.service.transform :as trans]
            [amjil.service.etl :as etl]))

(defn exec-process [data]
  ; (jdbc/insert-multi! db/sqlite :job_logs data)
  (doall
    (for [dat data]
      (let [job-done (jdbc/query db/sqlite ["select * from job_logs where batch_id = ?" (:batch_id dat)])]
        (if (empty? job-done)
          (do
            (log/warn "The Batch_id = " (:batch_id dat) " is inserting....")
            (jdbc/insert! db/sqlite :job_logs dat)
            (log/warn "The Batch_id = " (:batch_id dat) " is inserted....")

            (log/warn "The job_date = " (:job_date dat) " is starting....")
            (let [tables (-> (slurp "daily-all.txt") (str/split #"\r\n"))]
              (doall
                (map #(trans/transform "2" % (str/replace (:job_date dat) #"-" "")) tables)))
            (trans/transform-with-citys "nmart.rpt_long_stay_user_day")
            (trans/transform-with-lac "nmart.rpt_band_area_yx_daily")
            (trans/transform-with-lac "pmart.mid_scenery_arry_in_ter_day")
            (log/warn "The job_date = " (:job_date dat) " is endted...."))
          (log/warn "The Batch_id = " (:batch_id dat) " is already done......"))))))

(defn main-process [job-date]
  (let [data (jdbc/query db/tdcore ["SEL TXDate as job_date ,JOB_BATCH   as  batch_id FROM  PVIEW.VW_DSYNC_JOB_DONE where SYNC_DT= ? and JOB_NM='RPT_STAY_LEAVE_DAY'" job-date])]
    (if (empty? data)
      (log/warn "The ETL has not done yet.......")
      (exec-process data))))

(defjob TransformJob
  [ctx]
  (log/warn "TransormJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)]
    (log/warn "Transform params = " params)
    (main-process (:job_date params)))
  (log/warn "TransformJob....")
  (log/warn "TransormJob Ended ........"))

(defjob RealtimeJob
  [ctx]
  (log/warn "RealtimeJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)
        tables (-> (slurp "reatime.txt") (str/split #"\r\n"))]
    (log/warn "Reatime params = " params)
    (doall (map #(trans/transform "1" % (:job_time params)))))
  (log/warn "RealtimeJob Ended ........"))

(defjob DailyJob
  [ctx]
  (log/warn "DailyJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)
        tables (-> (slurp "daily-all.txt") (str/split #"\r\n"))]
    (log/warn "Daily params = " params)
    (doall (map #(trans/transform "2" % (:job_date params)))))
  (log/warn "DailyJob Ended ........"))

(defjob MonthJob
  [ctx]
  (log/warn "MonthJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)
        tables (-> (slurp "month.txt") (str/split #"\r\n"))]
    (log/warn "Month params = " params)
    (doall (map #(trans/transform "3" % (:job_month params)))))
  (log/warn "MonthJob Ended ........"))

(defjob ImportJob
  [ctx]
  (log/warn "ImportJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)]
    (log/warn "Import params = " params)
    (etl/import (:date params)))
  (log/warn "ImportJob Ended ........"))

(defjob ExportJob
  [ctx]
  (log/warn "ExportJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)]
    (log/warn "Export params = " params)
    (etl/export (:date params)))
  (log/warn "ExportJob Ended ........"))

(defjob TransJob
  [ctx]
  (log/warn "TransJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)]
    (log/warn "Trans params = " params)
    (etl/transformx (:date params)))
  (log/warn "TransJob Ended ........"))
