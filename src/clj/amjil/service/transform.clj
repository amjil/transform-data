(ns amjil.service.transform
  (:require [amjil.db :as db]
            [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

(defn table-column [table-name]
  (let [db-result (jdbc/query db/td [(str "help table " table-name)])
        stringified-cols (map #(get (clojure.walk/stringify-keys %) "column name") db-result)]
    (map #(-> % str/trim str/lower-case) stringified-cols)))

(defn column-string [table-name]
  (let [cols (table-column table-name)]
    (->> (map #(str (if (= "cal_date"%) "cast(cal_date as char(8))" %) " as " %) cols) (str/join ","))))

(defn cond-string [type _ columns]
  (cond
    (= "1" type) "1=1"
    (= "2" type) (if (not= -1 (.indexOf columns "cal_date")) "cal_date = ?" "1=1")
    (= "3" type) "cal_month = ?"))

(defn transform [type table-name cond-val]
  (log/warn "table-name = " table-name)
  (let [columns (table-column table-name)
        column-str (column-string table-name)
        cond-str (cond-string type table-name columns)
        cond-vec (if (not= -1 (.indexOf cond-str "?")) [cond-str cond-val] [cond-str])
        sql-str (str "select " column-str " from " (str/replace table-name #"kmart" "nmart") " where " cond-str)
        sql-vec (if (not= -1 (.indexOf cond-str "?")) [sql-str cond-val] [sql-str])]
    (log/warn (jdbc/delete! db/mysql (keyword table-name) cond-vec))
    (let [data (jdbc/query db/td sql-vec {:as-array? true})
          result-num (count (jdbc/insert-multi! db/mysql (keyword table-name) data))]
      (log/warn result-num)
      result-num)))

; (defn select-job-sync []
;   (let []))

(defn transform-all [table-name]
  (let [columns (table-column table-name)
        column-str (column-string table-name)
        sql-str (str "select " column-str " from " table-name)
        _ (log/warn "The table " table-name " starting....")
        del-num (jdbc/delete! db/mysql (keyword table-name) ["1=1"])
        data (jdbc/query db/td [sql-str] {:as-array? true})
        result-num (count (jdbc/insert-multi! db/mysql (keyword table-name) data))]
    (log/warn "del-num = " del-num "  result-num = " result-num)
    [del-num result-num]))

(defn transform-data [table-name]
  (let [columns (table-column (str/replace table-name #"kmart" "nmart"))
        column-str (column-string table-name)
        sql-str (str "select " column-str " from " (str/replace table-name #"kmart" "nmart") (if (not= -1 (.indexOf columns "cal_date")) " where cal_date = date - 2"))
        my-cond (if (not= -1 (.indexOf columns "cal_date")) "cal_date = date(now()) - 2" "1=1")
        _ (log/warn "The table " table-name " starting....")
        del-num (jdbc/delete! db/mysql (keyword table-name) [my-cond])
        data (jdbc/query db/td [sql-str] {:as-array? true})
        result-num (count (jdbc/insert-multi! db/mysql (keyword table-name) data))]
    (log/warn "del-num = " del-num "  result-num = " result-num)
    [del-num result-num]))

(def citys ["473" "476" "470" "479" "477" "482" "478" "475" "483" "474" "472" "471"])

(defn transform-with-citys [table-name date]
  (let [columns (table-column table-name)
        column-str (column-string table-name)
        sql-str (str "select " column-str " from " table-name (if (not= -1 (.indexOf columns "cal_date")) " where cal_date = ?" " where 1=1 ")  "and city_id = ?")
        flag (not= -1 (.indexOf columns "cal_date"))]
    (spit (str table-name ".txt") "")
    (map (fn [x] (let [data (drop 1 (jdbc/query db/td (if flag [sql-str date x] [sql-str x]) {:as-array? true}))]
                   (prn x)
                   (as-> (map #(str/join "\t" %) data) m
                     (str/join "\r\n" m)
                     (spit (str table-name ".txt") m :append true))))
        citys)))


(defn transform-with-lac [table-name]
  (let [column-str (column-string table-name)
        del-num (jdbc/delete! db/mysql (keyword table-name) ["cal_date = date(now()) - 2"])
        sql-str (str "select " column-str " from " table-name " where cal_date = date -2  and lac_id in (select lac_id from pmart.mid_scenery_hotel_any_info where city_id = ?)")]
    (log/warn "table name " table-name)
    (log/warn "del-num = " del-num)
    (doall
      (for [city citys]
        (let [data (jdbc/query db/td [sql-str city] {:as-array? true})
              result-num (count (jdbc/insert-multi! db/mysql (keyword table-name) data))]
          (log/warn city " " result-num)
          [city result-num])))
    (let [sql-str (str "select " column-str " from " table-name " where cal_date = date - 2 and lac_id not in (select lac_id from pmart.mid_scenery_hotel_any_info)")
          data (jdbc/query db/td [sql-str] {:as-array? true})
          result-num (count (jdbc/insert-multi! db/mysql (keyword table-name) data))]
      (log/warn " other " result-num)
      result-num)))
