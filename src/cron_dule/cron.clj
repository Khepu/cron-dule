(ns cron-dule.cron
  (:require
   [clojure.string :as string]
   [next.jdbc :as jdbc]
   [cron-dule.database :as db])
  (:import
   [java.util BitSet]))

(defrecord Cron
    [^BitSet seconds
     ^BitSet minutes
     ^BitSet hours
     ^BitSet days
     ^BitSet months
     ^BitSet weekdays])

(defn parse-range
  "Returns numeric [start end] tuple, assumes input is normalized to lowercase."
  ([^String range-str]
   (let [components (string/split range-str #"-")]
     (when (= 2 (count components))
       (let [[start end] components]
         [(Integer/parseInt start)
          (Integer/parseInt end)]))))

  ([^String named-range translator]
   (let [components (string/split named-range #"-")]
     (when (= 2 (count components))
       (let [[start end] components
             start (get translator start)
             end (get translator end)]
         (when-not start
           (throw (ex-info "No translation found for range start!"
                           {:range named-range :value [start end]})))
         (when-not end
           (throw (ex-info "No translation found for range end!"
                           {:range named-range :value [start end]})))
         [start end])))))

(defn -parse-fragment
  "Returns either a [start end] vector or an integer. `min` and `max` are inclusive."
  [^String fragment ^long min ^long max translator]
  (cond
    ;; if it's a wildcard then we don't need care about the rest of the
    ;; results but we do care about correctness!
    (= fragment "*") [min max]

    ;; this is a range. We need to:
    ;; 1. split the range
    ;; 2. parse components
    ;; 3. expand to values
    (string/index-of fragment \-)
    (if-let [[start end] (if (Character/isDigit (.charAt fragment 0))
                           (parse-range fragment)
                           (parse-range fragment translator))]
      (if (and (not (= start end))
               (<= min start end max))
        [start end]
        (throw (ex-info "Bad range!" {:fragment fragment
                                      :parsed [start end]})))
      ;; if there are more then it's invalid
      (throw (ex-info "Incomprehensible range!" {:fragment fragment})))

    ;; just a number
    :else
    (let [number (if (Character/isDigit (.charAt fragment 0))
                   (Integer/parseInt fragment)
                   (get translator fragment))]
      (if (and number (<= min number max))
        number
        (throw (ex-info "Invalid value!" {:fragment fragment
                                          :parsed number}))))))

(defn compact
  "Applies `value` (an integer or a vector of [start end]) to bitset and returns
  it. The single parameter implementation is required for transducers."
  ([^BitSet bitset value]
   (if (vector? value)
     (.set bitset (first value) (inc (second value)))
     (.set bitset value))
   bitset)
  ([^BitSet bitset] bitset))

(def months-translator
  {"jan"  1 "feb"  2 "mar"  3
   "apr"  4 "may"  5 "jun"  6
   "jul"  7 "aug"  8 "sep"  9
   "oct" 10 "nov" 11 "dec" 12})

(def weekdays-translator
  {"sun" 0 "mon" 1 "tue" 2 "wed" 3 "thu" 4 "fri" 5 "sat" 6})

(defmacro parser [[^long min ^long max] expression translator]
  `(transduce
    (map #(-parse-fragment % ~min ~max ~translator))
    compact
    ;; here, and in other places, we waste 1 bit for convenience
    (new BitSet ~(inc max))
    (string/split ~expression #",")))

(defn parse-seconds [^String expression]
  (parser [0 59] expression {}))

(defn parse-hours [^String expression]
  (parser [0 23] expression {}))

(defn parse-days [^String expression]
  (parser [1 31] expression {}))

(defn parse-months [^String expression]
  (parser [1 12] expression months-translator))

(defn parse-weekdays [^String expression]
  (parser [0 6] expression weekdays-translator))

(defn parse [cron-string]
  (let [segments (string/split (string/lower-case cron-string) #" ")]
    (if (= 6 (count segments))
      (let [[seconds minutes hours days months weekdays] segments]
        (Cron. (parse-seconds seconds)
               (parse-seconds minutes)
               (parse-hours hours)
               (parse-days days)
               (parse-months months)
               (parse-weekdays weekdays)))
      (throw (ex-info "Invalid number of segments!"
                      {:expression cron-string
                       :segments segments})))))

;;
;; DB
;;

(defn insert [name expression target]
  (jdbc/execute-one!
   @db/datasource
   ["insert into registered_crons (name, expression, target)
     values (?, ?, ?)
     returning *;"
    name
    expression
    target]))
