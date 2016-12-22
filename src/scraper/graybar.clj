(ns scraper.graybar
  (:require [reaver :refer [parse extract-from text attr extract]]
            [scraper.helpers :as h]
            [scraper.scraper :as s]
            ))

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

(def website {:name "graybar"
              :base-url "https://www.graybar.com"
              :start-path "/store/en/gb?cm_re=portalpage-_-body-_-newstore"
              :gather-links-fn #'gather-links
              :gather-info-fn #'gather-info
              })




(h/delete-repository website)
(h/init-repository website)

(def run-atom? (atom true))
(def link-thread (future
                   (h/process-link-thread website run-atom?)))
(def downloaded-thread (future
                         (h/process-downloaded-thread website run-atom?)))
(reset! run-atom? false)

(gather-info (-> (h/get-id website "downloaded" "/P/Item/PVF/5133710/") :body parse))
(h/save-parsed website
               (assoc
                (gather-info (-> (h/get-id website "downloaded" "/P/Item/PVF/5133710/") :body parse))
                :path "/P/Item/PVF/5133710/"))




(map
 (fn [x] (-> (h/get-all-next website x) :hits :total))
 ["link" "downloaded" "parsed" "error"])

[@h/links-processed @h/downloaded-processed]

(h/check-response-failure website "testing" {:a 3 :b 4} {:response nil} "error while saving")
(h/save website "error" {:path "testing"})




(def x (h/download-url (str (:base-url website) (:start-path website))))
(extract-from (-> x :body parse) ".container"
              [:title :path]
              "a.product_group_name.product_info" text
              "a.product_group_name.product_info" (attr "href")
              )

(def y (h/download-url "http://www.graybar.com/store/en/gb/tools-testing-and-measuring?cm_cr=hp_fcat-_-Web+Activity-_-hp_featured_category_1-_-hp_ftcat_c1-_-Tools%2C+Testing+and+Measuring-productNameLink"))
;; side bar categories
(extract-from (-> y :body parse) ".content_section li"
              [:title :path]
              "a .facetCountContainer" text
              "a" (attr "href")
              )

(def z (h/download-url "http://www.graybar.com/store/en/gb/tools-testing-and-measuring/meters-and-testing-equipment"))
(extract-from (-> z :body parse) ".product_name"
              [:title :path]
              "a" text
              "a" (attr "href")
              )

paging requires javascript ...





(gather-info (h/get-id website "downloaded" "/P/Item/PVF/5133710/"))
;; (:body (h/get-id website "downloaded" "/P/Item/PVF/5133710/"))


;; (h/process-downloaded-thread website run-atom?)
;; (h/process-downloaded-thread website run-atom?)
(+ 1 1)

(:name website)
(h/get-id website "link" (:start-path website))
(:created-on (h/get-id website "link" (:start-path website)))

(h/get-all-next website "link")
(h/get-all-next website "downloaded")
(-> (h/get-all-next website "downloaded") :hits :hits first :_source)
(-> (h/get-all-next website "downloaded") :hits :hits first :_source :state)
(h/get-all-next website "parsed")

(def x (h/download-url (str (:base-url website) "/P/Category/List/1956-Conduit-Fittings-Bodies-Outlet-Boxes--Accessories")))
(gather-links (-> x :body parse))

(h/next-unprocessed-link website)
(def x (h/next-unprocessed-link website))
(h/download-link website x)
(def x (h/download-link website x))
(keys x)
(:body x)
(:status x)
(:path x)
(:start-path website)
(h/next-unprocessed-downloaded website)
(:body (h/next-unprocessed-downloaded website))

;; (gather-links (h/next-unprocessed-downloaded website))
;; (def z (h/next-unprocessed-downloaded website))
;; (gather-links (-> z :body parse))
(gather-info (-> z :body parse))

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


