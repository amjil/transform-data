(ns amjil.env
  (:require [clojure.tools.logging :as log]))

(def defaults
  {:init
   (fn []
     (log/info "\n-=[transform-data started successfully]=-"))
   :stop
   (fn []
     (log/info "\n-=[transform-data has shut down successfully]=-"))
   :middleware identity})
