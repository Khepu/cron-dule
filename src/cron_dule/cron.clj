(ns cron-dule.cron
  (:require
   [clojure.string :as string]
   [clojure.set :as set]
   [next.jdbc :as jdbc]
   [cron-dule.database :as db]))

(defrecord Cron
    [seconds
     minutes
     hours
     days
     months
     weekdays])

(defn parse-range [range-str]
  (let [components (string/split range-str #"-")]
    (when (= 2 (count components))
      (let [[start end] components
            start (Integer/parseInt start)
            end   (Integer/parseInt end)]
        [start end]))))

(defn parse-named-range
  "Returns numeric [start end] tuple, assumes input is normalized to lowercase."
  [named-range translator]
  (let [components (string/split named-range #"-")]
    (when (= 2 (count components))
      (let [[start end] components
            start (get translator start)
            end (get translator end)]
        [start end]))))

(defn -parse-numeric
  "Returns one of: `:*`, a hash-set of integers, or a number. `min` and `max` are inclusive."
  [^String fragment min max]
  (cond
    ;; if it's a wildcard then we don't need care about the rest of the
    ;; results but we do care about correctness!
    (= fragment "*") :*
    ;; this is a range. We need to:
    ;; 1. split the range
    ;; 2. parse components
    ;; 3. expand to values
    (string/index-of fragment \-)
    (if-let [[start end] (parse-range fragment)]
      (if (and (not (= start end))
               (<= min start end max))
        (into #{} (range start (inc end)))
        (throw "Bad range!"))
      ;; if there are more then it's invalid
      (throw "Found incomprehensible range!"))
    ;; just a number
    :else
    (let [number (Integer/parseInt fragment)]
      (if (<= min number max)
        number
        (throw "Invalid value")))))

(defn -parse-named
  [^String fragment min max translator]
  (cond
    ;; if it's a wildcard then we don't need care about the rest of the
    ;; results but we do care about correctness!
    (= fragment "*") :*

    ;; this is a range. We need to:
    ;; 1. split the range
    ;; 2. parse components
    ;; 3. expand to values
    (string/index-of fragment \-)
    (if-let [[start end] (if (Character/isDigit (first fragment))
                           (parse-range fragment)
                           (parse-named-range fragment translator))]
      (if (and (not (= start end))
               (<= min start end max))
        (into #{} (range start (inc end)))
        (throw "Bad range!"))
      ;; if there are more then it's invalid
      (throw "Found incomprehensible range!"))

    ;; just a number
    :else
    (let [number (if (Character/isDigit (first fragment))
                   (Integer/parseInt fragment)
                   (get translator fragment))]
      (if (<= min number max)
        number
        (throw "Invalid value for seconds")))))

(defn compact [values value]
  ;; TODO: Replace hash-set with BitSet
  (if (set? value)
    (set/union values value)
    (conj values value)))

(defn compact-all [max values]
  (if (or (contains? values :*)
          (= max (count values)))
    :*
    values))

(defn parse-seconds [^String expression]
  (->> (string/split expression #",")
       (map #(-parse-numeric % 0 59))
       (reduce compact #{})
       (compact-all 60)))

(defn parse-hours [^String expression]
  (->> (string/split expression #",")
       (map #(-parse-numeric % 0 23))
       (reduce compact #{})
       (compact-all 24)))

(defn parse-days [^String expression]
  (->> (string/split expression #",")
       (map #(-parse-numeric % 1 31))
       (reduce compact #{})
       (compact-all 31)))

(def months-translator
  {"jan" 1
   "feb" 2
   "mar" 3
   "apr" 4
   "may" 5
   "jun" 6
   "jul" 7
   "aug" 8
   "sep" 9
   "oct" 10
   "nov" 11
   "dec" 12})

(defn parse-months [^String expression]
  (->> (string/split expression #",")
       (map #(-parse-named % 1 12 months-translator))
       (reduce compact #{})
       (compact-all 12)))

(def weekdays-translator
  {"sun" 0
   "mon" 1
   "tue" 2
   "wed" 3
   "thu" 4
   "fri" 5
   "sat" 6})

(defn parse-weekdays [^String expression]
  (->> (string/split expression #",")
       (map #(-parse-named % 0 6 weekdays-translator))
       (reduce compact #{})
       (compact-all 7)))

(defn parse [cron-string]
  (let [segments (string/split cron-string #" ")]
    (when (= 6 (count segments))
      (let [[seconds minutes hours days months weekdays] segments]
        (Cron. (parse-seconds seconds)
               (parse-seconds minutes)
               (parse-hours hours)
               (parse-days days)
               (parse-months months)
               (parse-weekdays weekdays))))))

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
