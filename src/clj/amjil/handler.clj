(ns amjil.handler
  (:require
            [amjil.route :as route-base]
            [compojure.core :refer [routes wrap-routes]]
            [compojure.route :as route]
            [amjil.env :refer [defaults]]
            [mount.core :as mount]
            [amjil.middleware :as middleware]))

(mount/defstate init-app
  :start ((or (:init defaults) identity))
  :stop  ((or (:stop defaults) identity)))

(mount/defstate app
  :start
  (middleware/wrap-base
    (routes
          #'route-base/routes
          (route/not-found
             "page not found"))))
