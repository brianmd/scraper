(ns scraper.elliott
  (:require [reaver :refer [parse extract-from text attr extract]]
            [scraper.helpers :as h]
            ;; [plain-scrape.scraper :as s]
            [semantic-csv.core :as sc]
            [clojure.data.csv :as cd-csv]

            [clojure.java.io :as io]))

(defn gather-links [parsed]
  (->>
   (concat
    ;; major category links (off main page)
    (extract-from parsed ".graphicMenu-Col"
                  [:title :path]
                  ".catTitle" text
                  ".catHdr" (attr "href"))

    ;; single product link (from major category page)
    (extract-from parsed ".searchInfo h3"
                  [:title :path]
                  "a > span" text
                  "a" (attr "href"))

    ;; pager at bottom of major category page
    (extract-from parsed ".pagination li"
                  [:title :path]
                  "a" text
                  "a" (attr "href"))
    )
   (filter #(and (:title %) (:path %)))
   set
   ))

(defn gather-info [parsed]
  (let [groups
        (extract-from parsed "dl.itemSpec dd dl"
                      [:title :val]
                      "dt" text
                      "dd" text)]
    (apply merge
     (map
      #(into {} (map vector (:title %) (:val %)))
      groups))))

(def website {:name "elliott"
              :base-url "https://www.elliottelectric.com"
              ;; :start-url "https://www.elliottelectric.com/P/Category"
              :start-path "/P/Category"
              ;; :classify-fn #'classify
              :gather-links-fn #'gather-links
              :gather-info-fn #'gather-info
              })


(def prod-fields ["UPC/EAN/GTIN" "Unit" "Long Description" "Manufacturer" "Manufacturer's Part Number" "abc"])
(def prod-keys (mapv keyword prod-fields))
(defrecord Pricing [upc price descript manuf part-num matnr])

;; (def all-prods
;;   (h/query-string-seq "elliott" "parsed" "_type:parsed"
;;                     {:_source prod-fields
;;                      }))
;; (nth all-prods 0)


;; add matnr
;;    breaks on 289th
;; write to csv (use semantic-csv)

(defn pricing-products []
  (->>
   (h/query-string-seq "elliott" "parsed" "_type:parsed"
                       {:_source prod-fields})
   (map :_source)
   (map (apply juxt prod-keys))
   (map #(apply ->Pricing %))
   (map #(let [upc (h/normalize-upc (:upc %))]
           (assoc %
                  :price (-> % :price h/parse-float)
                  :upc upc
                  ;; :matnr (h/upc->matnr upc)
                  )))
   ))
;; (first (pricing-products))
;; (h/upc->matnr "078677603745")
;; (h/upc->matnr "786685682846")

;; (first (h/query-string-seq "elliott" "parsed" "wire"))
;; (first (h/query-string-seq "ecommerce" "product" "_type:product"))
;; (h/query-string-seq "ecommerce" "product" "wire")
;; (take 2 (h/query-string-seq "ecommerce" "product" "wire"))
;; (first (h/query-string-seq "ecommerce" "product" "wire"))
;; (h/query-string-seq "ecommerce" "product" "_type:product")
;; (h/query-string-seq "ecommerce" "product" "_type:product"
;;                     {:_source prod-fields})
;; (h/website-index-name "ecommerce")


(defn matnrd-products
  []
  (map #(assoc % :matnr (h/upc->matnr (:upc %)))
       (pricing-products)))

;; (->> (matnrd-products) (filter #(not-empty (:upc %))) (take 2))
;; (->> (matnrd-products) (filter #(not (nil? (:matnr %)))) (take 2))

(defn write-csv
  []
  (with-open [out-file (io/writer "elliott.csv")]
    (->> ;; data
         ;; (sc/cast-with {:this #(-> % float str)})
         (matnrd-products)
         sc/vectorize
         (cd-csv/write-csv out-file)))
  )


;; (write-csv)




;; (let [keys prod-keys]
;;   (->>
;;    all-prods
;;    (map :_source)
;;    ;; (map #(assoc % :Unit (-> % :Unit h/parse-float)))
;;    (map (apply juxt keys))
;;    (map #(apply ->Pricing %))
;;    (map #(assoc % :price (-> % :price h/parse-float)))
;;    (map (fn [p]
;;           (let [len (count (:upc p))]
;;             (cond->
;;                 p
;;               (= len 3) (assoc :upc nil)
;;               (= len 14) (assoc :upc (subs (:upc p) 1 13)) ; nix country & check digits
;;               ))))
;;    ;; (map first)
;;    (take 2)
;;    ;; (filter #(= 3 (if (string? %) (count %))))
;;    ;; (map #(if (string? %) (count %)))
;;    ;; (remove nil?)
;;    ;; (take 28790)
;;    ;; set
;;    ))



;; (h/delete-repository website)
;; (h/init-repository website)









;; (def run-atom? (atom true))
;; (def link-thread (future
;;                    (h/process-link-thread website run-atom?)))
;; (def downloaded-thread (future
;;                          (h/process-downloaded-thread website run-atom?)))
;; (reset! run-atom? false)



;; (map
;;  (fn [x] (-> (h/get-all-next website x) :hits :total))
;;  ["link" "downloaded" "parsed" "error"])

;; [@h/links-processed @h/downloaded-processed]









;; (h/check-response-failure website "testing" {:a 3 :b 4} {:response nil} "error while saving")
;; (h/save website "error" {:path "testing"})


;; (gather-info (-> (h/get-id website "downloaded" "/P/Item/PVF/5133710/") :body parse))
;; (h/save-parsed website
;;                (assoc
;;                 (gather-info (-> (h/get-id website "downloaded" "/P/Item/PVF/5133710/") :body parse))
;;                 :path "/P/Item/PVF/5133710/"))

;; (gather-info (h/get-id website "downloaded" "/P/Item/PVF/5133710/"))
;; ;; (:body (h/get-id website "downloaded" "/P/Item/PVF/5133710/"))


;; (h/process-downloaded-thread website run-atom?)
;; (h/process-downloaded-thread website run-atom?)

;; (h/get-id website "link" (:start-path website))
;; (:created-on (h/get-id website "link" (:start-path website)))

;; (h/get-all-next website "link")
;; (h/get-all-next website "downloaded")
;; (-> (h/get-all-next website "downloaded") :hits :hits first :_source)
;; (-> (h/get-all-next website "downloaded") :hits :hits first :_source :state)
;; (h/get-all-next website "parsed")

;; (def x (h/download-url (str (:base-url website) "/P/Category/List/1956-Conduit-Fittings-Bodies-Outlet-Boxes--Accessories")))
;; (gather-links (-> x :body parse))

;; (h/next-unprocessed-link website)
;; (def x (h/next-unprocessed-link website))
;; (h/download-link website x)
;; (def x (h/download-link website x))
;; (keys x)
;; (:body x)
;; (:status x)
;; (:path x)
;; (:start-path website)
;; (h/next-unprocessed-downloaded website)
;; (:body (h/next-unprocessed-downloaded website))

;; ;; (gather-links (h/next-unprocessed-downloaded website))
;; ;; (def z (h/next-unprocessed-downloaded website))
;; ;; (gather-links (-> z :body parse))
;; (gather-info (-> z :body parse))

;; (h/save website "config" {:path "website" :doc website})
;; (h/save-link website {:path (:start-path website)})
;; (map #(h/save-link website %) (-> front-page :body parse gather-links))

;; (h/next-unprocessed-link website)
;; (h/get-path website "link" (:start-path website))
;; (h/get-path website "link" "sss")
;; (h/get-all-next website "link" "new")
;; (h/get-all-next-unprocessed website "link")

;; (h/update-state website "link" (h/get-path website "link" (:start-path website)) "new")

;; (h/get-path website "link" (:start-path website))
;; {:path "/P/Category", :updated-on "2016-12-03T16:40:30Z", :state "new", :created-on "2016-12-03T16:40:30Z", :state-updated-on "2016-12-03T16:40:30Z"}


;; (def front-page (h/download-url (:start-path website)))
;; (def front-page (h/download-path website (:start-path website)))
;; (def major-category-page (h/download-url "https://www.elliottelectric.com/P/Category/List/1956-Conduit-Fittings-Bodies-Outlet-Boxes--Accessories"))
;; (def product-page (h/download-url "https://www.elliottelectric.com/P/Item/BRI/106S/"))

;; (->> front-page :body parse (gather-website-links website))
;; (-> front-page :body parse gather-links)
;; (-> major-category-page :body parse gather-links)
;; (-> product-page :body parse product-page-group-names)
;; (-> product-page :body parse gather-stats)


