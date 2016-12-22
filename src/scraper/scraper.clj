(ns scraper.scraper
  (:require
            [clojurewerkz.elastisch.rest :as esr]
            ;; [clojurewerkz.elastisch.native :as es]
            ;; [clojurewerkz.elastisch.rest.index :as esi]
            ;; [clojurewerkz.elastisch.rest.index :as esi]
            ;; [clojurewerkz.elastisch.rest.document :as esd]
            ;; [clojurewerkz.elastisch.rest.response :as esrsp]
            ;; [clojurewerkz.elastisch.query :as q]
            ;; [clojure.pprint :as pp]

            ;; [clj-time.core :as t :refer [now]]
            ;; [clj-time.coerce :as c]
            ;; ;; [clj-time.local :as l]

            [scraper.helpers :as h]
            ))

;; the repository will have the following tables:
;;   - link, for the links that are to be downloaded.
;;     this list grows as pages have links to other pages
;;   - downloaded, for links that have been downloaded
;;   - processed, for downloaded pages that have been processed
;;
;; each document in the above tables has a :state variable.
;; the state variable can be:
;;   - :new (has not been processed)
;;   - :processing (is currently being processed)
;;   - :processed (has completed processing)
;;   - :error (in which case, :error-reason will have more info)

(def example-link
  {:path "/a/b"
   :state :new  ; :processing :processed :error-downloading :error-parsing
   :state-updated-on "now"
   :requested-on "..."
   :downloaded-on "..."
   :parsed-on "..."
   :body "......."
   :sublinks ["/a/b/c" "/c"]
   :vitals {:upc ".." :price 2.43 :uom "EA"}
   }
  )

(def example-page
  {:path "/a/b"
   ;; :
   })




;; (def website "elliott")
;; (def repo-url "http://127.0.0.1:9200")
;; (def repo
;;   (esr/connect
;;    repo-url
;;    {:connection-manager
;;     (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 10})}))

;; (h/save  repo website "downloaded" {:path "/ab/cd" :a 9 :q 99})
;; (h/save  repo website "downloaded" {:path "/ab/cd" :a 17})
;; (esd/get repo website "downloaded" "/ab/cd")
;; (h/next-unprocessed-link repo website)
;; (h/next-unprocessed-downloaded repo website)

;; {:_index "elliot", :_type "downloaded", :_id "/ab/cd", :_version 3, :found true, :_source {:path "/ab/cd", :a 17, :state "new", :state-updated-on "2016-12-01T21:14:00Z"}}



(defn update-as-processing
  [page]
  )

(defn process-next-downloaded
  [website]
  (let [link (h/next-unprocessed-link repo website)]
    ))

(defn process-all-downloaded
  [website]
  (loop []
      (when (process-next-downloaded website)
        (recur))))

(defn gather-links
  [website link page]
  )

(defn download-next [website]
  (let [link (h/next-unprocessed-link repo website)
        page (if link (h/download-url (h/build-url website link)))]
    (try
      (when page
        (h/save-downloaded repo website link page)
        (h/update-state repo website link :processed)
        (try
          (gather-links website link page)
          (catch Exception e
            (h/update-state repo website link :error
                          {:error-state :gathering-links :error-msg (str e)}))))
      (catch Exception e
        (h/update-state repo website link :error
                      {:error-state :gathering-links :error-msg (str e)}))
      )
    link)
    )

(defn crawl
  "crawl the site, downloading pages as it goes"
  [website start-page process-fn options]
  (h/init-repository repo website)
  (let [opts (merge
              {:delay-ms 2000
               :process-downloaded true
               }
              options)]
    (loop []
        (when (download-next website)
          (Thread/sleep (:delay-ms opts))
          (when (:process-downloaded options)
            (process-next-downloaded website)
            (process-next-downloaded website))
          (recur)))))
