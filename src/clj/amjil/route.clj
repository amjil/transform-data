(ns amjil.route
  (:require [compojure.route :refer [not-found resources]])
  (:require [compojure.core :refer [defroutes GET POST PUT context wrap-routes]]
            [amjil.controller :as controller]))


(def routes (compojure.core/routes
                 (GET "/reatime" {params :params} (controller/realtime params))
                 (GET "/daily" {params :params} (controller/daily params))
                 (GET "/month" {params :params} (controller/month params))
                 (GET "/sync" {params :params} (controller/sync-job params))
                 (GET "/unlock-batch" {params :params} (controller/unlock-batch params))
                 (GET "/create-table" [] (controller/create-table))
                 (GET "/transform" {params :params} (controller/transform params))
                 (GET "/export" {params :params} (controller/export params))
                 (GET "/export-once" {params :params} (controller/export-once params))
                 (GET "/export-table" {params :params} (controller/export-table params))
                 (GET "/export-big" {params :params} (controller/export-big params))
                 (GET "/import" {params :params} (controller/import params))
                 (GET "/import-table" {params :params} (controller/import-table params))
                 (GET "/import-url" {params :params} (controller/import-url params))
                 (GET "/check" {params :params} (controller/check params))))
