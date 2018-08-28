(ns amjil.controller
  (:require [clojure.tools.logging :as log]
            [ring.util.response :refer [response content-type charset]]
            [amjil.db :as db]
            [amjil.job.base :as job-base]
            [amjil.job.job :as job-main]
            [amjil.service.etl :as etl]
            [clj-time.local :as local-time]
            [clj-time.format :as time-format]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]))

(defn test [params]
  (-> (response "test success ") (content-type "text/text") (charset "utf-8")))

(defn transform [params]
  (let [current-time (time-format/unparse (time-format/formatters :basic-date-time) (local-time/local-now))]
    (job-base/trigger-job amjil.job.job.TransJob
                              params
                              (str "trans." current-time)
                              (str "trans." current-time)))
  (-> (response "trans trigger success ") (content-type "text/text") (charset "utf-8")))

(defn export-once [params]
  (clojure.java.io/make-parents "once/a.txt")
  (let [tables (-> (slurp "once.txt") (str/split #"\r\n"))]
    (doall (map #(etl/export-data % (:date params)) tables)))
  (-> (response "export trigger success ") (content-type "text/text") (charset "utf-8")))

(defn export [params]
  (let [current-time (time-format/unparse (time-format/formatters :basic-date-time) (local-time/local-now))]
    (job-base/trigger-job amjil.job.job.ExportJob
                              params
                              (str "export." current-time)
                              (str "export." current-time)))
  (-> (response "export trigger success ") (content-type "text/text") (charset "utf-8")))

(defn export-big [params]
  (let [[table-name date] (str/split (:params params) #":")]
    (etl/export-big table-name date))
  (-> (response "export big table success ") (content-type "text/text") (charset "utf-8")))

(defn export-table [params]
  (let [[type table-name date] (str/split (:params params) #":")]
    (etl/export-table type table-name date))
  (-> (response "export table success ") (content-type "text/text") (charset "utf-8")))

(defn import-url [params]
  (let [[dir type table date] (str/split (:params params) #":")]
    (log/warn params)
    (etl/import-url dir type table date))
  (-> (response "import table success ") (content-type "text/text") (charset "utf-8")))

(defn import-table [params]
  (let [[type table date] (str/split (:params params) #":")]
    (etl/import-datax type table date))
  (-> (response "import table success ") (content-type "text/text") (charset "utf-8")))

(defn import [params]
  (let [current-time (time-format/unparse (time-format/formatters :basic-date-time) (local-time/local-now))]
    (job-base/trigger-job amjil.job.job.ImportJob
                              { "date" (:date params)}
                              (str "import." current-time)
                              (str "import." current-time)))
  (-> (response "import trigger success ") (content-type "text/text") (charset "utf-8")))

(defn create-table []
  (let [sql (jdbc/create-table-ddl :job_logs [[:id :integer :primary :key :autoincrement][:batch_id :text] [:job_date :date] [:job_nm :text]])]
    (jdbc/execute! db/sqlite [sql]))
  (-> (response "create table success ") (content-type "text/html") (charset "utf-8")))

(defn check [params]
  (let [data (jdbc/query db/tdcore ["SEL TXDate as job_date ,JOB_BATCH   as  batch_id FROM  PVIEW.VW_DSYNC_JOB_DONE where SYNC_DT= ? and JOB_NM='RPT_STAY_LEAVE_DAY'" (:date params)])]
    (if (empty? data)
      (log/warn "The ETL has not done yet for job_date = " (:date params) ".......")
      (doall
        (for [dat data]
          (let [job-done (jdbc/query db/sqlite ["select * from job_logs where batch_id = ?" (:batch_id dat)])]
            (if (empty? job-done)
              (log/warn "The job_date = " (:job_date dat) " is dosn't sync....")
              (log/warn "The Batch_id = " (:batch_id dat) " is already done......")))))))
  (-> (response "check success") (content-type "text/text") (charset "utf-8")))

(defn unlock-batch [params]
  (let [result (jdbc/delete! db/sqlite :job_logs ["batch_id = ?" (:batch params)])]
    (-> (response (str "success " result)) (content-type "text/html") (charset "utf-8"))))

(defn sync-job [params]
  (let [current-time (local-time/local-now)
        job-params (:jobparams params)
        [job-type job-date] (str/split job-params #":")]
    (job-base/trigger-job amjil.job.job.TransformJob
                              { "job_type" job-type "job_date" job-date}
                              (str "sync." current-time)
                              (str "sync." current-time)))
  (-> (response "success") (content-type "text/html") (charset "utf-8")))

(defn realtime [params]
  (let [current-time (local-time/local-now)
        job-params (:jobparams params)
        [job-type job-time] (str/split job-params #":")]
    (job-base/trigger-job amjil.job.job.RealtimeJob
                              {"job_type" job-type "job_time" job-time}
                              (str "daily." current-time)
                              (str "daily." current-time)))
  (-> (response "success") (content-type "text/html") (charset "utf-8")))

(defn daily [params]
  (let [current-time (local-time/local-now)
        job-params (:jobparams params)
        [job-type job-date] (str/split job-params #":")]
    (job-base/trigger-job amjil.job.job.DailyJob
                              { "job_type" job-type "job_date" job-date}
                              (str "daily." current-time)
                              (str "daily." current-time)))
  (-> (response "success") (content-type "text/html") (charset "utf-8")))

(defn month [params]
  (let [current-time (local-time/local-now)
        job-params (:jobparams params)
        [job-type job-month] (str/split job-params #":")]
    (job-base/trigger-job amjil.job.job.MonthJob
                              {"current_time" current-time "job_type" job-type
                               "job_month" job-month}
                              (str "month." current-time)
                              (str "month." current-time)))
  (-> (response "success") (content-type "text/html") (charset "utf-8")))
