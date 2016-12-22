(ns scraper.exporter
  (:require ;; [clojurewerkz.elastisch.rest :as esr]
            ;; [clojurewerkz.elastisch.rest.index :as index]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            ;; [clojurewerkz.elastisch.rest.admin :as admin]
            [clojurewerkz.elastisch.rest.index :as esi]
            ;; [clojurewerkz.elastisch.rest.document :as esd]
            ;; [clojurewerkz.elastisch.rest.response :as esrsp]
            ;; [clojurewerkz.elastisch.query :as q]
            ;; [clojure.pprint :as pp]

            ;; [reaver :refer [parse extract-from text attr extract]]
            ;; [clj-http.client :as client]

            [scraper.helpers :as h]
            ))

(defn indices
  [repo]
  (esi/get-mapping repo "_all" "_all")
  ;; (-> (index/stats repo nil) :indices keys)
  )

;; (def stats (-> (index/stats h/repo nil) :indices :elliott))
;; (def stats (-> (index/stats h/repo nil) :indices :elliott))
;; (esi/get-mapping h/repo "elliott" "downloaded")
;; (-> (esi/get-mapping h/repo "elliott" "downloaded") :indices)
;; (-> stats keys)
;; (-> stats :primaries)
;; (-> stats :primaries :docs :count)
;; (-> stats :primaries keys)
;; (-> stats :total keys)



(defn index-names
  [repo]
  (-> (indices repo) keys))

(defn types
  [repo index-name]
  (let [index (keyword index-name)]
    (-> repo indices index :mappings)))

(defn type-names
  [repo index-name]
  (-> (types repo index-name) keys))

(defn properties
  [repo index-name type-name]
  (let [type (keyword type-name)]
    (-> (types repo index-name) type :properties)))

(defn property-names
  [repo index-name type-name]
  (-> (properties repo index-name type-name) keys))


(defn export-type
  "directory name must end with a '/'"
  [repo index-name type-name directory]
  (let [filename (str directory (name index-name) "-" (name type-name))]
    (println "filename:" filename)
    (with-open [w (io/writer filename :encoding "UTF-8")]
      (println "opened" index-name type-name)
      (doseq [doc (h/query-all-seq index-name type-name)]
        (println ".")
        (println (-> doc :_source :path))
        (.write w (json/generate-string doc))
        (.write w "\n")
        )
      )))
(export-type h/repo :elliott :error "backup/")
(export-type h/repo :elliott :downloaded "backup/")

(defn backup-index
  [index-name]
  (doseq [type (type-names h/repo index-name)]
    (println "backup:" type)
    (export-type h/repo index-name type "backup/")))

(backup-index :elliott)
(export-type h/repo :elliott :downloaded "backup/")

;; (export-type h/repo "elliott" "parsed" "backup/")
;; (export-type h/repo "elliott" "link" "backup/")
;; (export-type h/repo "elliott" "error" "backup/")
;; (export-type h/repo "elliott" :error "backup/")

;; (indices h/repo)
;; (index-names h/repo)
;; (types h/repo :elliott)
;; (type-names h/repo :elliott)
(:link :parsed :error :downloaded)
;; (types h/repo "elliott")
;; (types h/repo "elliott")
;; (properties h/repo :elliott :parsed)
;; (property-names h/repo :elliott :parsed)

