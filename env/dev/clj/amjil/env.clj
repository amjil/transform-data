(ns amjil.env
  (:require
            [clojure.tools.logging :as log]
            [amjil.dev-middleware :refer [wrap-dev]]))

(def defaults
  {:init
   (fn []
     (log/info "\n-=[transform-data started successfully using the development profile]=-"))
   :stop
   (fn []
     (log/info "\n-=[transform-data has shut down successfully]=-"))
   :middleware wrap-dev})
