(ns user
  (:require [amjil.config :refer [env]]
            [clojure.spec.alpha :as s]
            [expound.alpha :as expound]
            [mount.core :as mount]
            [amjil.core :refer [start-app]]
            [amjil.db :as db]))

(alter-var-root #'s/*explain-out* (constantly expound/printer))

(defn start []
  (mount/start-without #'amjil.core/repl-server))

(defn stop []
  (mount/stop-except #'amjil.core/repl-server))

(defn restart []
  (stop)
  (start))
