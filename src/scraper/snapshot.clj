(ns scraper.snapshot
  (:require [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.admin :as admin]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.query :as q]
            [clojure.pprint :as pp]

            [reaver :refer [parse extract-from text attr extract]]
            [clj-http.client :as client]

            [scraper.helpers :as h]
            ))


(defn register
  []
  (admin/register-snapshot-repository
   h/repo "my_backup"
   {:type "fs"
    :settings {:compress true :location "/var/ek/backups/my_backup"}}))

(defn snapshot
  [snapshot-name]
  (admin/take-snapshot
   h/repo (str "my_backup/" snapshot-name)))
