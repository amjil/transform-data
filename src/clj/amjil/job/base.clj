(ns amjil.job.base
  (:require [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs :as j :refer [defjob]]
            [clojurewerkz.quartzite.schedule.daily-interval :as daily-interval]
            [clojurewerkz.quartzite.schedule.simple :as s]
            [clojurewerkz.quartzite.schedule.cron :as cron]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clj-time.local :as local-time]
            [clj-time.core :as time]
            [ring.util.response :refer [response content-type charset]]
            [amjil.db :as db]))

(defn quartz
  []
  (qs/initialize))

(defn build-job
  [JobClass job-name job-data]
  (j/build (j/of-type JobClass)
           (j/using-job-data job-data)
           (j/with-identity (j/key job-name))))

(defn build-trigger
  ; [trigger-id group-id]
  [trigger-id]
  (t/build
    (t/start-now)
    ; (t/with-identity trigger-id group-id)
    (t/with-identity (t/key trigger-id))
    (t/with-schedule (s/schedule
                       (s/with-repeat-count 0)
                       (s/with-interval-in-seconds 5)))))

(defn trigger-job
  [JobClass job-data job-name trigger-id]
  (let [quartz (quartz)
        job (build-job JobClass job-name job-data)
        trigger (build-trigger trigger-id)]
    (qs/schedule quartz job trigger)))

(defjob SubJob
  [ctx]
  (let [current-time (local-time/local-now)]
    (prn "sub job ============" current-time)))

; (defjob MainJob
;   [ctx]
;   (log/warn "The MainJob starting............")
;   (let [current-time (local-time/local-now)
;         settle-date (str (time/month current-time) (time/day current-time))
;         merchants (db/get-merchants-for-union)]
;     (log/warn "main job ============" current-time "  " settle-date)
;     (log/warn "main job ------------" merchants)
;     (for [merchant merchants]
;       (let [job-data {"settleDate" settle-date "merId" merchant}
;             job-name (str "job." merchant "." settle-date)
;             trigger-name (str "trigger." merchant "." settle-date)]
;         (trigger-job amjil.job.union.UnionSettleJob job-data job-name trigger-name))))
;   (log/warn "The MainJob ending............"))
;
; (defn runjob
;   []
;   (let [s   (-> (qs/initialize) qs/start)
;         job (j/build
;               (j/of-type clj.job.base.MainJob)
;               (j/with-identity (j/key "jobs.01")))
;         trigger (t/build
;                   (t/start-now)
;                   (t/with-identity (t/key "triggers.01"))
;                   (t/with-schedule
;                     (cron/schedule
;                       (doto (cron/cron-schedule "0 0 2 * * ?")
;                         (cron/with-misfire-handling-instruction-fire-and-proceed)
;                         (cron/in-time-zone (java.util.TimeZone/getTimeZone "Asia/Shanghai"))))))]
;     (qs/schedule s job trigger)))

; (runjob)

(defn start-quartz
  []
  (-> (qs/initialize) qs/start))

; (defn stop-quartz
;   []
;   (-> (qs/initialize) qs/stop))
