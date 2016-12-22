(defproject scraper "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-http "3.4.1"]
                 [clj-time "0.12.2"]
                 [clojurewerkz/elastisch "3.0.0-beta1"]
                 [org.elasticsearch/elasticsearch "2.3.3"]
                 ;; [enlive "1.1.6"]
                 [cheshire "5.6.3"]
                 [reaver "0.1.2"]
                 [semantic-csv "0.1.0"]
                 [org.clojure/data.csv "0.1.3"]
                 ]
  ;; :main ^:skip-aot plain-scrape.core
  :main ^:skip-aot scraper.helpers
  :target-path "target/%s"
  ;; :jvm-opts ["-Xss800m" "-Xms900m"]
  :profiles {:uberjar {:aot :all}})
