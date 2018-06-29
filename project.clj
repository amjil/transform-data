(defproject transform-data "0.1.0"

  :description "FIXME: write description"
  :url "http://example.com/FIXME"

  :dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                 [clj-time "0.14.4"]
                 [clojurewerkz/quartzite "2.1.0"]
                 [compojure "1.6.1"]
                 [cprop "0.1.11"]
                 [funcool/struct "1.3.0"]
                 [luminus-http-kit "0.1.6"]
                 [luminus-nrepl "0.1.4"]
                 [metosin/ring-http-response "0.9.0"]
                 [mount "0.1.12"]
                 [mysql/mysql-connector-java "8.0.11"]
                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.6"]
                 [org.clojure/tools.cli "0.3.7"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.xerial/sqlite-jdbc "3.23.1"]
                 [ring-logger "1.0.1"]
                 [ring/ring-core "1.6.3"]
                 [ring/ring-json "0.4.0"]
                 [td/tdgs "1.0.0"]
                 [td/tdjdbc "1.0.0"]]

  :min-lein-version "2.0.0"
  ; :jvm-opts ["-Xmx20g"]
  :jvm-opts ["-Xmx10g"]

  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :resource-paths ["resources"]
  :target-path "target/%s/"
  :aot [clojure.tools.logging.impl clojurewerkz.quartzite.conversion]
  :main ^:skip-aot amjil.core

  :plugins [[lein-ancient "0.6.15"]]

  :profiles
  {:uberjar {:omit-source true
             :aot :all
             :uberjar-name "transform-data.jar"
             :source-paths ["env/prod/clj"]
             :resource-paths ["env/prod/resources"]}

   :dev           [:project/dev :profiles/dev]
   :test          [:project/dev :project/test :profiles/test]

   :project/dev  {:jvm-opts ["-Dconf=dev-config.edn"]
                  :dependencies [[expound "0.7.0"]
                                 [pjstadig/humane-test-output "0.8.3"]
                                 [prone "1.6.0"]
                                 [ring/ring-devel "1.6.3"]
                                 [ring/ring-mock "0.3.2"]]
                  :plugins      [[com.jakemccrary/lein-test-refresh "0.19.0"]]

                  :source-paths ["env/dev/clj"]
                  :resource-paths ["env/dev/resources"]
                  :repl-options {:init-ns user}
                  :injections [(require 'pjstadig.humane-test-output)
                               (pjstadig.humane-test-output/activate!)]}
   :project/test {:jvm-opts ["-Dconf=test-config.edn"]
                  :resource-paths ["env/test/resources"]}
   :profiles/dev {}
   :profiles/test {}})
