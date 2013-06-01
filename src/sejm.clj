(ns sejm
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]
            [clojure.string :as string])
  (:use [net.cgrand.enlive-html :only [html-resource select attr? text emit*]]))

(defn url-listy-poslow [kadencja]
  (format "http://orka.sejm.gov.pl/ArchAll2.nsf/%sRP"
          (["pierwsza" "druga" "trzecia" "czwarta" "piata" "szosta"] (dec kadencja))))

(defn select-url
  [url & selectors]
  (apply select (html-resource (io/reader url :encoding "ISO-8859-2")) selectors))

(defn poslowie [kadencja]
  (map #(vector (-> % :content first) (-> % :attrs :href))
       (select-url (url-listy-poslow kadencja) [:tr (attr? :target)])))

(defn dane-posla [url-part]
  [(string/trim (text (first (select-url (str "http://orka.sejm.gov.pl" url-part) [:td.Klub]))))])

(defn dane-poslow
  ([] (apply concat (map dane-poslow (range 1 7))))
  ([kadencja]
     (for [[posel url] (poslowie kadencja)]
       (into [(str kadencja) posel] (dane-posla url)))))

(defn dump []
  (spit "/tmp/poslowie.csv" (csv/write-csv (dane-poslow))))