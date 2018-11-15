(ns amjil.route
  (:require [compojure.route :refer [not-found resources]])
  (:require [compojure.core :refer [defroutes GET POST PUT context wrap-routes]]
            [amjil.controller :as controller]))


(def routes (compojure.core/routes
                 (GET "/unlock-batch" {params :params} (controller/unlock-batch params))
                 (GET "/create-table" [] (controller/create-table))
                 (GET "/transform" {params :params} (controller/transform params))
                 (GET "/export" {params :params} (controller/export params))
                 (GET "/export-async" {params :params} (controller/export-async params))
                 (GET "/import" {params :params} (controller/import params))
                 (GET "/import-async" {params :params} (controller/import-async params))
                 (GET "/import-url" {params :params} (controller/import-with-file-url params))
                 (GET "/check" {params :params} (controller/check params))
                 (GET "/test" {params :params} (controller/test params))))
