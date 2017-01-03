(ns scraper.helpers
  (:require [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.query :as q]
            [clojure.pprint :as pp]

            [reaver :refer [parse extract-from text attr extract]]
            [clj-http.client :as client]
            ))

(def index-settings
  {:settings {:number_of_shards 1
              :number_of_replicas 2}
   :mappings {"elliott" {:properties
                         {:body {:type "string"
                                 :index "not_analyzed"}}}}})

(def repo-url (if-let [url (System/getenv "ELASISEARCH_URL")]
                url
                "http://127.0.0.1:9200"
                ;; "http://192.168.0.220:9201"
                ;; "http://murphydye.com:9200"
                ))
(def repo
  (esr/connect
   repo-url
   {:connection-manager
    (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 10})}))

(defn to-s [obj]
  (condp contains? (type obj)
    #{clojure.lang.Keyword clojure.lang.Symbol java.lang.String} (name obj)
    #{Long Integer} (str obj)
    (str obj)))
;; (to-s "a")
;; (to-s :a)
;; (to-s 'a)
;; (to-s 2)

(defn website-index-name [website]
  (if (map? website)
    (clojure.string/join "_" (map to-s (remove nil? [(:name website) (:env website) (:version website)])))
    (to-s website)))
;; (website-index-name {:name "boo" :version 2 :env :prod})

(defn download-url
  "return response from url's content"
  ([url] (download-url url {:socket-timeout 10000 :conn-timeout 1000}))
  ([url options]
   (let [response (client/get url options)]
     response)))
;; (download-url "http://google.com/")

(defn download-link
  ([website link] (download-link website link {}))
  ([website link options]
   (prn website)
   (download-url (str (:base-url website) (:path link)) options)))





;; -------------------------------  repository document functions

(defn timenow []
  (java.util.Date.))



(defn get-first-doc [response]
  (-> response :hits :hits first :_source))

(defn get-id [website type id]
  (let [index (website-index-name website)
        response (esd/get repo index type id)]
    (when response
      (:_source response))))

(defn exists? [website type doc]
  (if (get-id website type (:path doc))
    true
    false))

(defn get-all-next
  ([website type]
   (esd/search repo (website-index-name website) type
               {:query {:match_all {}}
                :size 1}))
  ([website type state]
   (esd/search repo (website-index-name website) type
               {:query {:match {:state state}}
                :size 1})))

(defn query-all-seq
  ([website type] (query-all-seq website type {}))
  ([website type options]
   (esd/scroll-seq repo
                   (esd/search repo (website-index-name website) (name type)
                               (merge
                                {:query {:match_all {}}
                                 :scroll "1m"
                                 :size 1000}
                                options)))))
;; (take 2 (query-all-seq "ecommerce" "product"))

(defn query-string-seq
  ([website type qstring] (query-string-seq website type qstring {}))
  ([website type qstring options]
   (if (empty? qstring)
     (query-all-seq website type options)
     (let [response
           (esd/scroll-seq repo
                           (esd/search repo (website-index-name website) (name type)
                                       (merge
                                        {:query {:query_string {:query qstring}}
                                         :scroll "1m"
                                         :size 1000}
                                        options)))]
       response))))

;; (take 2 (esd/scroll-seq repo
;;          (esd/search repo "ecommerce" "product"
;;                      {:query {:query_string {:query "wire"}}
;;                       :scroll "1m"
;;                       :size 1000})))
;; (take 2 (query-string-seq "ecommerce" "product" "wire"))
;; (take 2 (query-string-seq "ecommerce" "product" ""))
;; (take 2
;;       (let [query-string "wire"
;;             website "ecommerce"]
;;         (esd/scroll-seq repo
;;                         (esd/search repo (website-index-name website) type
;;                                     {:query {:query_string {:query query-string}}
;;                                      :scroll "1m"
;;                                      :size 1000}
;;                                     ))))

(defn parse-long [s]
  (if (string? s)
    (if-let [token (re-find #"\d+" s)]
      (if (string? token)
        (Long. token)))))
(defn parse-float [s]
  (if (string? s)
    (if-let [token (re-find #"\d+\.?\d+" s)]
      (if (string? token)
        (Double. token)))))
;; (parse-float "$2.32 ea")
;; (parse-float "$2.32 ea 44.2")

(defn query-string
  ([website type qstring] (query-string website type qstring {}))
  ([website type qstring options]
   (let [response
         (esd/search repo (website-index-name website) type
                     (merge
                      {:query {:query_string {:query qstring}}
                       :size 2}
                      options))
         ]
     response)))
;; (query-string "ecommerce" "product" "upc:\"032664576553\"" {})

(defn query-string-and-hits
  ([website type qstring] (query-string-and-hits website type qstring {}))
  ([website type qstring options]
   (let [response (query-string website type qstring options)]
     [(-> response :hits :total)
      (->>
       response
       :hits :hits
       (map :_source)
       (map #(dissoc % :error-state))
       )])))

(defn query-string-docs
  ([website type qstring] (query-string website type qstring {}))
  ([website type qstring options]
   (second (query-string-and-hits website type qstring options))))

(defn normalize-upc [upc]
  (when upc
    (let [len (count upc)]
      (cond
        (= len 3) nil
        ;; (= len 13) (subs upc 0 12) ; check digits
        ;; (= len 14) (subs upc 1 13) ; nix country & check digits
        (= len 13) (subs upc 1 13) ; check digits
        (= len 14) (subs upc 2 14) ; nix country & check digits
        :else upc
        ))))
;; (normalize-upc "032664576553")
;; (normalize-upc "0326645765534")
;; (normalize-upc "00326645765534")

(defn upc->matnr [upc]
  (when upc
    (->
     (query-string "ecommerce" "product" (str "upc:" upc))
     :hits :hits first :_source :matnr parse-long
     )))
;; (upc->matnr "032664576553")    ;; has two matnrs!
;; (upc->matnr "786685682846")
;; (upc->matnr "008870053894")



(defn get-all-next-unprocessed
  [website type]
  (get-all-next website type "new"))

(defn get-next-unprocessed
  [website type]
  (let [one (get-all-next-unprocessed website type)]
    (if (<= 1 (-> one :hits :total))
      (-> one :hits :hits first :_source)
      nil
      )))

(defn next-unprocessed-link
  [website]
  (get-next-unprocessed website "link"))

(defn next-unprocessed-downloaded
  [website]
  (get-next-unprocessed website "downloaded"))



(declare save-error)

(defn check-response-failure
  [website typee doc response msg]
  (let [failed (-> response :_shards :failed)
        right-type? (contains? #{java.lang.Long java.lang.Integer} (type failed))]
    (when (or (not right-type?) (< 0 failed))
      (println "error, check-response-failure" typee doc response msg)
      (save-error website {:msg msg :type typee :doc doc :response response})
      )))

(defn save
  [website type doc]
  {:pre [(contains? doc :path)]}
  (let [index (website-index-name website)
        now (timenow)
        response (esd/put repo index type (:path doc)
                 (assoc
                  (cond-> doc
                    (not (contains? doc :created-on)) (assoc :state :new :created-on now)
                    (not (contains? doc :state)) (assoc :state :new :state-updated-on now)
                    )
                  :updated-on now))]
    (def x2 response)
    (check-response-failure website type doc response "error while saving")
    response))


(defn create
  [website type doc]
  (let [index (website-index-name website)
        now (timenow)
        response (esd/create repo index type
                    (assoc
                     (cond-> doc
                       (not (contains? doc :created-on)) (assoc :state :new :created-on now)
                       )
                     :updated-on now))]
    (check-response-failure website type doc response "error while creating")
    response))




(defn save-error
  [website doc]
  (create website "error" doc))

(defn save-new
  [website type doc]
  (when (nil? (exists? website type doc))
    (save website type doc)))

(defn save-link
  [website doc]
  (save website "link" doc))

(defn create-link
  [website doc]
  (when-not (exists? website "link" doc)
    (save-link website doc)))

(defn save-downloaded
  [website doc]
  (save website "downloaded" doc))

(defn create-downloaded
  [website doc]
  (when-not (exists? website "downloaded" doc)
    (save-downloaded website doc)))

(defn save-parsed
  [website doc]
  (save website "parsed" doc))

(defn update-state
  ([website type doc new-state] (update-state website type doc new-state {}))
  ([website type doc new-state options]
   {:pre (contains? doc :path)}
   (save website type
         (merge
          options
          (assoc doc :state new-state :state-updated-on (timenow))))
   )
  )


;; ---------------------------- processing

(defn process-link
  [website link]
  1 / 0
  (try
    (let [response (download-link website link)]
      (if (= (:status response) 200)
        (do
          (create-downloaded website (merge
                                    response
                                    {:path (:path link)}
                                    ))
          (update-state website "link" link :processed))
        (do
          (update-state website "link" link :error {:error-msg "download error" :error-response response})
          (save-error website {:fn :process-link :doc link :response response}))
        )
      )
    (catch Exception e
      (println "error in process-link" link (str e))
      (update-state website "link" link :error {:error-state (str e)})
      (save-error website {:fn :process-link :doc link :msg (str e)})
      :error
      )))

(defn process-downloaded
  [website downloaded]
  (try
    (let [parsed (-> downloaded :body parse)
          links ((:gather-links-fn website) parsed)
          info ((:gather-info-fn website) parsed)
          ]
      (def lll links)
      (def mmm (map #(create-link website %) links))
      (doall mmm)
      (when info (save-parsed website (assoc info :path (:path downloaded))))
      (update-state website "downloaded" downloaded :processed)
      )
    (catch Exception e
      (println "error in process-downloaded" downloaded (str e))
      (update-state website "downloaded" downloaded :error {:error-state (str e)})
      (save-error website {:fn :process-downloaded :doc downloaded :msg (str e)})
      )))

;; lll
;; mmm

(defn process-next-link
  [website]
  (when-let [l (next-unprocessed-link website)]
    (process-link website l)))


(defn process-next-downloaded
  [website]
  (when-let [d (next-unprocessed-downloaded website)]
    (process-downloaded website d)))

(defonce links-processed (atom 0))
(defonce downloaded-processed (atom 0))

;; [@links-processed @downloaded-processed]

(defn process-link-thread
  [website run-atom?]
  (while @run-atom?
    (swap! links-processed inc)
    (process-next-link website)
    (Thread/sleep 4000)
    ))
(defn process-link-thread
  [website run-atom?]
  (while @run-atom?
    (swap! links-processed inc)
    (process-next-link website)
    (Thread/sleep 3800)
    ))

(defn process-downloaded-thread
  [website run-atom?]
  (loop []
    (swap! downloaded-processed inc)
    (process-next-downloaded website)
    (Thread/sleep 2000)
    (if @run-atom? (recur))))


;; ----------------------------- repository index functions

(defn init-repository
  "initialize scraper index. throw error if unable to create it (and it did not already exist)"
  [website]
  {:pre [(contains? website :name)
         (contains? website :start-path)]}
  (try
    (esi/create repo (website-index-name website) index-settings)
    (catch Exception e
      (println "index already exists" e)
      (when-not (->> e ex-data :body (re-find #"already exists"))
        (throw e))))
  (if (exists? website "link" {:path "start-path"})
    :start-path-already-existed
    (do
      (save-link website {:title "start-path" :path (:start-path website)})
      ;; :created
      )))

(defn delete-repository
  "delete scraper index. throw error if unable to delete it (and it didn't exist)"
  [website]
  {:pre [(contains? website :name)]}
  (try
    (let [response (esi/delete repo (website-index-name website))]
      (cond
        (= (:acknowledged response) true) :deleted
        (= (-> response :error :type) "index_not_found_exception") :did-not-exist
        :else (throw (str "error deleting repository " website))
        ))
    (catch Exception e
      (throw e)
      )))





;; ------------------------------- errors

(defn delete-errors
  "delete from index-name 'error' with fn:link-process"
  [website query-string]
  (let [index (website-index-name website)
        error-ids
        (map :_id (-> (query-string index "error" query-string {:size 9999}) :hits :hits))]
    (map #(esd/delete repo index "error" %) error-ids)))

(defn retry-link-errors
  "delete-error-links first"
  [website]
  (delete-errors website "fn:link-process")
  (let [index (website-index-name website)
        link-ers
         (query-string-docs index
                            "link" "+state:error" {:size 2000})]
    (doall
     (map #(save index "link" (assoc % :state "new")) link-ers))
     ))
;; (count link-ers)
;; (def link-errs
;;   (set
;;    (map :path (query-string-docs "elliott" "link" "state:error" {:size 2000}))))
;; (count link-errs)

;; (def error-ids
;;   (set
;;    (map :_id (-> (query-string "elliott" "error" "fn:link-process" {:size 2000}) :hits :hits))))
;; (def error-links
;;   (set
;;    (map (comp :path :doc) (query-string-docs "elliott" "error" "fn:link-process" {:size 2000}))))
;; (count error-links)
;; (clojure.set/difference link-errs error-links)
;; (clojure.set/difference error-links link-errs)
;; (= error-links link-errs)

;; (doall
;;  (map #(esd/delete repo "elliott" "error" %) error-ids))

;; (doall
;;  (map #(esd/update
;;         )))

;; (esd/put repo "elliott" "error" "testing" {:a 3 :b 4})
;; (esd/put repo "elliott" "error" "testing" {:c 5})
;; (esd/get repo "elliott" "error" "testing")

;; (count (query-string-docs "elliott" "error" "fn:link-process" {:size 30}))
;; (query-string-and-hits "elliott" "error" "fn:link-process")



;; ------------


;; (defn slink [query-string]
;;   (let [response
;;         (esd/search repo "elliott" "link"
;;                     {:query {:query_string {:query query-string}}
;;                      ;; :size 1
;;                      })]
;;     [(-> response :hits :total)
;;      (->>
;;       response
;;       :hits :hits
;;       (map :_source)
;;       (map #(dissoc % :error-state))
;;       ;; (map :path)
;;       )]))

;; (slink "state:error")

;; (def link-errs
;;   (->> (slink "state:error")
;;        second
;;        (map :path)
;;        ))
;; (count link-errs)


;; (slink "state:error AND path:category")
;; (slink "state:error AND path:'category list'")
;; (slink "state:error AND path:'category list 1956 conduit 10 outlet boxes'")
;; (-> (slink "state:error AND path:'category list 1956 conduit 10 outlet boxes'") second first :path)
;; (def pp (-> (slink "state:error AND path:'category list 1956 conduit 10 outlet boxes'") second first :path))
;; (get-id {:name "elliott"} "link" p)
;; (esd/get repo "elliott" "link" p)
;; "/P/Category/List/1956-Conduit-Fittings-Bodies-Outlet-Boxes--Accessories?CurrentPage=10"

;; (gid "elliott" "link" p)
;; (:_source (gid "elliott" "link" p))
;; (defn gid [website type id]
;;   (esd/get repo website type id))


;; (->>
;;  (esd/search repo "elliott" "link"
;;              {:query {:query_string {:query "state:error AND path:category"}}
;;               ;; :size 1
;;               })
;;  :hits :hits
;;  (map :_source)
;;  (map #(dissoc % :error-state))
;;  ;; (map :path)
;;  )


;; (->
;;  (esd/search repo "elliott" "link"
;;              {:query {:match {:state "error"
;;                               ;; :path "category"
;;                               }}
;;               :size 1})
;;  :hits :hits first :_source
;;  (dissoc :error-state)
;;  )

;; (->
;;  (esd/search repo "elliott" "error"
;;              {:query {:match {:fn :process-link}}
;;               :size 1})
;;  :hits :hits first :_source :doc :path)

;; (esd/search repo "elliott" "link"
;;             {:query {:match {:fn :process-link}}
;;              :size 1})

;; (def p "/P/Category/List/1956-Conduit-Fittings-Bodies-Outlet-Boxes--Accessories?CurrentPage=10")
;; (get-id {:name "elliot"}
;;         "link"
;;         p)


