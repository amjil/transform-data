(ns amjil.db
  (:require [mount.core :as mount]
            [amjil.config :refer [env]]
            [clojure.java.jdbc :as jdbc]))


(mount/defstate ^{:on-reload :noop} td
  :start
  {:datasource (doto (com.teradata.jdbc.TeraDataSource.)
                    (.setDatabaseName "NGBASS")
                    (.setUser (:td-username env))
                    (.setpassword (:td-password env))
                    (.setDSName (:td-server env))
                    (.setCLIENT_CHARSET "cp936")
                    (.setDbsPort (:td-port env)))})

(mount/defstate ^{:on-reload :noop} fast
  :start
  {:datasource (doto (com.teradata.jdbc.TeraDataSource.)
                    (.setDatabaseName "NGBASS")
                    (.setUser (:td-username env))
                    (.setpassword (:td-password env))
                    (.setDSName (:td-server env))
                    (.setTYPE "FASTEXPORT")
                    (.setCLIENT_CHARSET "cp936")
                    (.setDbsPort (:td-port env)))})

(mount/defstate ^{:on-reload :noop} tdcore
  :start
  {:datasource (doto (com.teradata.jdbc.TeraDataSource.)
                    (.setDatabaseName "NGBASS")
                    (.setUser (:tdcore-username env))
                    (.setpassword (:tdcore-password env))
                    (.setDSName (:tdcore-server env))
                    (.setCLIENT_CHARSET "cp936")
                    (.setDbsPort (:tdcore-port env)))})

(mount/defstate ^{:on-reload :noop} mysql
  :start
  { :classname "com.mysql.cj.jdbc.Driver"
    :subprotocol "mysql"
    :subname (:mysql-subname env)
    :user (:mysql-username env)
    :password (:mysql-password env)})

;
(mount/defstate ^{:on-reload :noop} sqlite
  :start
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "db/database.db"})

;
(extend-protocol jdbc/IResultSetReadColumn java.sql.Date (result-set-read-column [col _ _] (str col)))
