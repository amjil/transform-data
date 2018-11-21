(ns amjil.util.import
  (:require [clojure.java.shell :as shell]
            [amjil.config :refer [env]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]))

(defn import [type file date table]
  (let [sh-out (shell/sh "etlload.sh" type file date table)]
    (if-not (= 0 (:exit sh-out))
      (log/error "Shell status = " (:exit sh-out))
      (log/warn "Shell status = " (:exit sh-out)))
    (log/warn "Shell output = " (:out sh-out))))
