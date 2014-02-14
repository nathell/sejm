(ns sejm
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]
            [clojure.string :as string])
  (:use [net.cgrand.enlive-html :only [html-resource select attr? text emit* first-child last-child]]))

(defn url-listy-poslow [kadencja]
  (format "http://orka.sejm.gov.pl/ArchAll2.nsf/%sRP"
          (["pierwsza" "druga" "trzecia" "czwarta" "piata" "szosta"] (dec kadencja))))

(let [mutex (Object.)]
  (defn log [& args]
    (let [s (apply format args)]
      (locking mutex
        (println s)
        (flush)))))

(defn resource
  ([url] (resource url "ISO-8859-2"))
  ([url encoding]
     (log "Downloading data from %s" url)
     (html-resource (io/reader url :encoding encoding))))

(defn select-url
  [url & selectors]
  (apply select (resource url) selectors))

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

;; głosowania z kadencji 3-7

(defn url-glosowania [kadencja posiedzenie glosowanie]
  (format "http://www.sejm.gov.pl/sejm7.nsf/agent.xsp?symbol=glosowania&NrKadencji=%d&NrPosiedzenia=%d&NrGlosowania=%d" kadencja posiedzenie glosowanie))

(defn wyniki-glosowania-klubu [[klub url]]
  (let [url (str "http://www.sejm.gov.pl/sejm7.nsf/" url)
        res (resource url "UTF-8")
        wyniki (select res [:div#contentBody :tbody :tr :td text])]
    (for [[_ posel wynik] (partition 3 wyniki)]
      [klub posel wynik])))

(defn wyniki-glosowania [kadencja posiedzenie glosowanie]
  (let [res (resource (url-glosowania kadencja posiedzenie glosowanie) "UTF-8")
        party-anchors (select res [:div#contentBody :tbody :tr :> first-child :a])
        party-links (for [a party-anchors] [(text a) (:href (:attrs a))])
        datetime (first (select res [:div#title_content :small text]))
        [_ date time] (when datetime (re-find #"Dnia (.*) godz\. (.*)" datetime))]
    {:title (first (select res [:div#contentBody :div.sub-title :big text])),
     :subtitle (first (select res [:div#contentBody :div.sub-title :p.subbig :font text])),
     :datetime (str date " " time),
     :results (mapcat wyniki-glosowania-klubu party-links)}))

(defn zapisz-wyniki-glosowania-csv 
  [outdir kadencja posiedzenie glosowanie]
  (let [header-file-name (format "%s/header-%s-%s-%s.csv" outdir kadencja posiedzenie glosowanie)
        results-file-name (format "%s/results-%s-%s-%s.csv" outdir kadencja posiedzenie glosowanie)]
    (if (and (.exists (io/file header-file-name)) (.exists (io/file results-file-name)))
      (log "Data already downloaded for %s %s %s, skipping" kadencja posiedzenie glosowanie)
      (let [wgl (wyniki-glosowania kadencja posiedzenie glosowanie)]
        (spit header-file-name 
              (csv/write-csv [(map str [kadencja posiedzenie glosowanie (:datetime wgl) (:title wgl) (:subtitle wgl)])]))
        (spit results-file-name
              (csv/write-csv (map (partial into [(str kadencja) (str posiedzenie) (str glosowanie)]) (:results wgl))))))))

;; Adapted from http://grokbase.com/t/gg/clojure/125absae3d
(defn partition-when
  "Applies f to each value in coll, splitting it each time f returns
a truthy value. Returns a lazy seq of partitions."
  [f coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (let [run (cons (first s) (take-while (complement f) (next s)))]
       (cons run (partition-when f (seq (drop (count run) s))))))))

(defn url-posiedzenia [kadencja]
  (format "http://www.sejm.gov.pl/sejm7.nsf/agent.xsp?symbol=posglos&NrKadencji=%s" kadencja))

(defn posiedzenia [kadencja]
  (let [res (resource (url-posiedzenia kadencja) "UTF-8")
        liczby (select res [:div#contentBody :tbody :tr :> #{first-child last-child} text])
        podzialy (partition-when #(re-find #"\d" (first %)) (partition 2 liczby))]
    (for [p podzialy]
      [(Integer/parseInt (first (first p)))
       (apply + (map #(Integer/parseInt (second %)) p))])))

(defn download-votes [outdir]
  (let [input-data
        (for [kadencja (range 7 2 -1)
              [posiedzenie nglos] (posiedzenia kadencja)
              glosowanie (range 1 (inc nglos))]
          [kadencja posiedzenie glosowanie])]
    (dorun
     (pmap
      (fn [g]
        (try
          (apply zapisz-wyniki-glosowania-csv outdir g)
          (catch Exception e
            (log "Unable to download data for %s" (pr-str g)))))
      input-data))))
  
