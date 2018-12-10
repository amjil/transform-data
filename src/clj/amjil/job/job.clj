(ns amjil.job.job
  (:require [clojurewerkz.quartzite.jobs :as j :refer [defjob]]
            [clojurewerkz.quartzite.conversion :as qc]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [amjil.db :as db]
            [amjil.service.etl :as etl]
            [amjil.util.import :as im]
            [amjil.util.etl :as ex]))


(defjob ImportJob
  [ctx]
  (log/warn "ImportJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)
        [type table date] (str/split (:params params) #":")
        filename (str "./data/" date "/" table ".txt")]
    (log/warn "Import params = " params)
    (im/import type filename date table))
  (log/warn "ImportJob Ended ........"))

(defjob ExportJob
  [ctx]
  (log/warn "ExportJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)
        [table-name date] (str/split (:params params) #":")]
    (log/warn "Export params = " params)
    (ex/etl-export date table-name))
  (log/warn "ExportJob Ended ........"))

(defjob TransformJob
  [ctx]
  (log/warn "TransformJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)]
    (log/warn "Transform params = " params)
    (etl/transformx (:date params)))
  (log/warn "TransformJob Ended ........"))

(defjob TransJob
  [ctx]
  (log/warn "TransJob starting...... ")
  (let [params (-> (qc/from-job-data ctx) walk/keywordize-keys)
        [table-name date] (str/split (:params params) #":")]
    (log/warn "Trans params = " params)
    (ex/etl-transaction date table-name))
  (log/warn "TransJob Ended ........"))
