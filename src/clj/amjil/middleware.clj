(ns amjil.middleware
  (:require [amjil.env :refer [defaults]]
            [amjil.config :refer [env]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.content-type :refer [wrap-content-type]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            ; [ring.middleware.json :refer [wrap-json-params wrap-json-body wrap-json-response]]
            [ring.logger :as logger]
            [ring.util.response :refer [response content-type charset]]
            [clojure.tools.logging :as log]))

(defn wrap-base [handler]
  (-> ((:middleware defaults) handler)
    ;; common wrapper
    wrap-keyword-params
    wrap-params
    logger/wrap-with-logger))
