(ns amjil.service.etl
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [amjil.db :as db]
            [amjil.config :refer [env]]
            [amjil.util.etl :as etl]
            [amjil.util.sql :as sql]
            [clj-time.format :as time-format]))

(defn transformx [date]
  (let [data (sql/query-td-job-log date)]
    (if (empty? data)
      (log/warn "The ETL  date = " date " has not done yet.......")
      (doall
        (for [d data]
          (let [job-done (sql/query-sqlite-job-log d)
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
