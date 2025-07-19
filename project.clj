(defproject cron-dule "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [org.clojure/data.json "2.5.1"]

                 [aleph "0.8.3"]
                 [metosin/reitit "0.9.1"]

                 [org.postgresql/postgresql "42.7.7"]
                 [com.github.seancorfield/next.jdbc "1.3.1048"]]

  :main ^:skip-aot cron-dule.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
