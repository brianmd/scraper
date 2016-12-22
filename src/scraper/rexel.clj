(ns scraper.rexel
  (:require [reaver :refer [parse extract-from text attr extract]]
            [scraper.helpers :as h]
            ))

(defn gather-links [parsed]
  (->>
   (concat
    ;; front page links
    (extract-from parsed ".IT33_shop_products span"
                  [:title :path]
                  "a" text
                  "a" (attr "href"))

    ;; category and sub-category page links
    (extract-from parsed "ul dt"
                  [:title :path]
                  "a" text
                  "a" (attr "href"))
    )
   (filter #(and (:title %) (:path %)))
   set
   ))

(defn gather-info [parsed]
  (let [bits
        (concat
         (extract-from parsed ".prodDetailsInfo .prodPrice h2"
                       [:price]
                       "span" text)

         (extract-from parsed ".priceInputCont h4"
                       [:unit-multiple]
                       "abbr" text)

         (extract-from parsed "div.prodDesc > span"
                       [:descript]
                       "h4" text)

         ;; table of detail info
         (into {}
               (map #(vector (:title %) (:val %))
                    (extract-from parsed ".prodInfoTabDet .tabDetailsInfo h1"
                                  [:title :val]
                                  "span:nth-of-type(1)" text
                                  "span:nth-of-type(2)" text
                                  )))
         )]
    (apply merge bits)))
(gather-info parsed)
(-> parsed gather-info keys)
("Material" "Type" :unit-multiple :descript "Opening" "Mfr / Brand" "UPC" "Rexel Part #" "UNSPSC" "Mfr Part #" :price)



(def website {:name "rexel"
              :env :prod
              ;; :version 2
              :base-url "https://www.rexelusa.com"
              :start-path "/"
              :gather-links-fn #'gather-links
              :gather-info-fn #'gather-info
              })






(def prod-fields ["Material" "Type" :unit-multiple :descript "Opening" "Mfr / Brand" "UPC" "Rexel Part #" "UNSPSC" "Mfr Part #" :price])

(def prod-keys (mapv keyword prod-fields))
(defrecord Pricing [upc price descript manuf part-num matnr])

;; (def all-prods
;;   (h/query-string-seq "elliott" "parsed" "_type:parsed"
;;                     {:_source prod-fields
;;                      }))
;; (nth all-prods 0)


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

;; (def priced-products
;;   (pricing-products))

;; (def matnrd-products
;;   (doall
;;    (map #(assoc % :matnr (h/upc->matnr (:upc %)))
;;         priced-products)))
;; (take 5 matnrd-products)
;; (last priced-products)
;; (count priced-products)



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



(h/delete-repository website)
(h/init-repository website)









(def run-atom? (atom true))
(def link-thread (future
                   (h/process-link-thread website run-atom?)))
(def downloaded-thread (future
                         (h/process-downloaded-thread website run-atom?)))
(reset! run-atom? false)



(map
 (fn [x] (-> (h/get-all-next website x) :hits :total))
 ["link" "downloaded" "parsed" "error"])

[@h/links-processed @h/downloaded-processed]









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


;; (def front-page (h/download-url (str (:base-url website) (:start-path website))))
;; (def parsed (-> front-page :body parse))
;; (extract-from parsed ".IT33_shop_products span"
;;               [:title :path]
;;               "a" text
;;               "a" (attr "href"))
;; (gather-links parsed)

;; (def category-page (h/download-url "https://www.rexelusa.com/boxes-enclosures/boxes-explosionproof/category/41"))
;; (def parsed (-> category-page :body parse))
;; (extract-from parsed "ul dt"
;;               [:title :path]
;;               "a" text
;;               "a" (attr "href"))
;; (gather-links parsed)

;; (def subcategory-page (h/download-url "https://www.rexelusa.com/boxes-enclosures/boxes-explosionproof/conduit-junction-boxes/category/1167"))
;; (def parsed (-> subcategory-page :body parse))
;; (extract-from parsed "ul dt"
;;               [:title :path]
;;               "a" text
;;               "a" (attr "href"))

;; (def products-page (h/download-url "https://www.rexelusa.com/boxes-enclosures/boxes-explosionproof/conduit-outlet-boxes/type-lb/category/3208"))
;; (def parsed (-> products-page :body parse))
;; (set
;;  (extract-from parsed "#pagination fieldset dl dd"
;;                [:title :path]
;;                "a" text
;;                "a" (attr "href")))



;; gather-info

(def product-page (h/download-url "https://www.rexelusa.com/grss-type/egs/grss100/appleton-grss100-conduit-outlet-box-type-grss-explosionproof-dust-ignitionproof/product/45170"))
(def parsed (-> product-page :body parse))

(let [bits
      (concat
       (extract-from parsed ".prodDetailsInfo .prodPrice h2"
                     [:price]
                     "span" text)

       ;; (clojure.string/split "abc def g" #" ")
       (extract-from parsed ".priceInputCont h4"
                     [:unit-multiple]
                     "abbr" text)

       (extract-from parsed "div.prodDesc > span"
                     [:descript]
                     "h4" text)

       ;; table of detail info
       (extract-from parsed ".prodInfoTabDet .tabDetailsInfo h1"
                     [:title :val]
                     "span:nth-of-type(1)" text
                     "span:nth-of-type(2)" text
                     ))]
  (apply merge bits))


(extract-from parsed ".prodInfoTabDet .tabDetailsInfo h1"
              [:title :val]
              "span:nth-of-type(1)" text
              "span:nth-of-type(2)" text
              )
(into {}
      (map #(vector (:title %) (:val %))
           (extract-from parsed ".prodInfoTabDet .tabDetailsInfo h1"
                         [:title :val]
                         "span:nth-of-type(1)" text
                         "span:nth-of-type(2)" text
                         )))
(keys (into {}
            (map #(vector (:title %) (:val %))
                 (extract-from parsed ".prodInfoTabDet .tabDetailsInfo h1"
                               [:title :val]
                               "span:nth-of-type(1)" text
                               "span:nth-of-type(2)" text
                               ))))
("Mfr / Brand" "Mfr Part #" "Rexel Part #" "UPC" "UNSPSC" "Material" "Opening" "Type")


;; (def front-page (h/download-path website (:start-path website)))
;; (def major-category-page (h/download-url "https://www.elliottelectric.com/P/Category/List/1956-Conduit-Fittings-Bodies-Outlet-Boxes--Accessories"))
;; (def product-page (h/download-url "https://www.elliottelectric.com/P/Item/BRI/106S/"))

;; (->> front-page :body parse (gather-website-links website))
;; (-> front-page :body parse gather-links)
;; (-> major-category-page :body parse gather-links)
;; (-> product-page :body parse product-page-group-names)
;; (-> product-page :body parse gather-stats)


