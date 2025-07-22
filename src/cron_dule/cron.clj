(ns cron-dule.cron
  (:require
   [clojure.string :as string]
   [next.jdbc :as jdbc]
   [cron-dule.database :as db])
  (:import
   [java.util BitSet]
   [java.time LocalDateTime ZoneId DayOfWeek Year]
   [clojure.lang PersistentVector LongRange]))

(set! *warn-on-reflection* true)

(defrecord Cron
    [^BitSet seconds
     ^BitSet minutes
     ^BitSet hours
     ^BitSet days
     ^BitSet months
     ^BitSet weekdays])


(defn parse-range
  "Returns numeric [start end] tuple, assumes input is normalized to lowercase."
  [^String range-expr translator]
  (let [components (string/split range-expr #"-")]
    (when (= 2 (count components))
      (let [[^String start ^String end] components
            start (if (Character/isDigit (.charAt start 0))
                    (Integer/parseInt start)
                    (get translator start))
            end (if (Character/isDigit (.charAt end 0))
                  (Integer/parseInt end)
                  (get translator end))
            start-end [start end]]
        (when-not start
          (throw (ex-info "No translation found for range start!"
                          {:fragment range-expr :value start-end})))
        (when-not end
          (throw (ex-info "No translation found for range end!"
                          {:fragment range-expr :value start-end})))
        start-end))))

(defn parse-step
  [^String step-expr translator]
  (let [components (string/split step-expr #"/")]
    (when (= 2 (count components))
      (let [[^String start ^String step] components
            start (if (Character/isDigit (.charAt start 0))
                    (Integer/parseInt start)
                    (if (= start "*")
                      0
                      (get translator start)))
            step (Integer/parseInt step)
            start-step [start step]]
        (when-not start
          (throw (ex-info "No translation found for step start!"
                          {:fragment step-expr :value start-step})))
        (when-not step
          (throw (ex-info "No translation found for step end!"
                          {:fragment step-expr :value start-step})))
        start-step))))

(defn parse-fragment
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
    (if-let [[start end] (parse-range fragment translator)]
      (if (and (not (= start end))
               (<= min start end max))
        [start end]
        (throw (ex-info "Bad range!"
                        {:fragment fragment :parsed [start end]})))
      ;; if there are more then it's invalid
      (throw (ex-info "Incomprehensible range!"
                      {:fragment fragment})))

    ;; this is a step expression. Similar to range.
    (string/index-of fragment \/)
    (if-let [[start step] (parse-step fragment translator)]
      (if (and (pos? step)
               (<= min start max))
        (range start (inc max) step)
        (throw (ex-info "Bad step!"
                        {:fragment fragment :parsed [start step]})))
      ;; if there are more then it's invalid
      (throw (ex-info "Incomprehensible step!" {:fragment fragment})))

    ;; just a number
    :else
    (let [number (if (Character/isDigit (.charAt fragment 0))
                   (Integer/parseInt fragment)
                   (get translator fragment))]
      (if (and number (<= min number max))
        number
        (throw (ex-info "Invalid value!"
                        {:fragment fragment :parsed number}))))))

(defn compact
  "Applies `value` (an integer or a vector of [start end]) to bitset and returns
  it. The single parameter implementation is required for transducers."
  ([^BitSet bitset value]
   (condp = (type value)
     PersistentVector
     (let [[^long a ^long b] value]
       (.set bitset a (inc b)))

     LongRange
     (doseq [pos value]
       (.set bitset pos))

     Integer
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

(defmacro parse [[^long min ^long max] expression translator]
  `(transduce
    (map #(parse-fragment % ~min ~max ~translator))
    compact
    ;; for some cases we waste 1 bit for convenience
    (new BitSet ~(inc max))
    (string/split ~expression #",")))

(defn parse-seconds [^String expression]
  (parse [0 59] expression {}))

(defn parse-minutes [^String expression]
  (parse [0 59] expression {}))

(defn parse-hours [^String expression]
  (parse [0 23] expression {}))

(defn parse-days [^String expression]
  (parse [1 31] expression {}))

(defn parse-months [^String expression]
  (parse [1 12] expression months-translator))

(defn parse-weekdays [^String expression]
  (parse [0 6] expression weekdays-translator))

(defn parse-cron
  ^Cron [^String cron-string]
  (let [segments (string/split (string/lower-case cron-string) #" ")]
    (if (= 6 (count segments))
      (let [[seconds minutes hours days months weekdays] segments]
        (Cron. (parse-seconds seconds)
               (parse-minutes minutes)
               (parse-hours hours)
               (parse-days days)
               (parse-months months)
               (parse-weekdays weekdays)))
      (throw (ex-info "Invalid number of segments!"
                      {:expression cron-string
                       :segments segments})))))

(def ^ZoneId zone (ZoneId/of "UTC"))

(defn utc-now
  ^LocalDateTime []
  (LocalDateTime/now zone))

(def day-of-week
  {DayOfWeek/SUNDAY    0
   DayOfWeek/MONDAY    1
   DayOfWeek/TUESDAY   2
   DayOfWeek/WEDNESDAY 3
   DayOfWeek/THURSDAY  4
   DayOfWeek/FRIDAY    5
   DayOfWeek/SATURDAY  6})

(defn next-slot [^BitSet bitset current-value]
  (let [next-value (.nextSetBit bitset current-value)]
    (if (<= 0 next-value)
      next-value
      (.nextSetBit bitset 0))))

(defn days-in-month
  ^long [^LocalDateTime local-date-time]
  (let [year (.getYear local-date-time)
        year-obj (Year/of year)
        month (.getMonth local-date-time)]
    (.length month (.isLeap year-obj))))

(defn next-execution
  ^LocalDateTime [^Cron cron]
  (let [now (utc-now)
        current-year (.getYear now)]
    (loop [exec-time now]
      (let [seconds (.getSecond exec-time)
            next-seconds (next-slot (:seconds cron) seconds)
            dist-seconds (if (< next-seconds seconds)
                           (- (+ 60 next-seconds) seconds)
                           (- next-seconds seconds))
            next-exec-time (if (zero? dist-seconds)
                             exec-time
                             (.plusSeconds exec-time dist-seconds))
            minutes (.getMinute next-exec-time)
            next-minutes (next-slot (:minutes cron) minutes)
            dist-minutes (if (< next-minutes minutes)
                           (- (+ 60 next-minutes) minutes)
                           (- next-minutes minutes))
            next-exec-time (if (zero? dist-seconds)
                             next-exec-time
                             (.plusMinutes next-exec-time dist-minutes))
            hour (.getHour next-exec-time)
            next-hour (next-slot (:hours cron) hour)
            dist-hour (if (< next-hour hour)
                        (- (+ 24 next-hour) hour)
                        (- next-hour hour))
            next-exec-time (if (zero? dist-hour)
                             next-exec-time
                             (.plusHours next-exec-time dist-hour))
            day (.getDayOfMonth next-exec-time)
            next-day (next-slot (:days cron) day)
            dist-day (if (< next-day day)
                       (- (+ (days-in-month next-exec-time) next-day) day)
                       (- next-day day))
            next-exec-time (if (zero? dist-day)
                             next-exec-time
                             (.plusDays next-exec-time dist-day))
            month (.getMonthValue next-exec-time)
            next-month (next-slot (:months cron) month)
            dist-month (if (< next-month month)
                         (- (+ 12 next-month) month)
                         (- next-month month))
            next-exec-time (if (zero? dist-month)
                             next-exec-time
                             (.plusMonths next-exec-time dist-month))
            ;; If we are not on the right weekday, then all we want to do is
            ;; jiggle things to the right weekday and the rest will converge
            weekday (get day-of-week (.getDayOfWeek next-exec-time))
            next-weekday (next-slot (:weekdays cron) weekday)
            dist-weekday (if (< next-weekday weekday)
                           (- (+ 7 next-weekday) weekday)
                           (- next-weekday weekday))
            next-exec-time (if (zero? dist-weekday)
                             next-exec-time
                             (.plusDays next-exec-time dist-weekday))]
        ;; Set am arbitrary stoping poing in case we hit a case
        ;; that doesn't exit like Febraury 30th
        (when (> (.getYear next-exec-time) (+ current-year 400))
          (throw (ex-info "Invalid cron expression" {:day day
                                                     :next-day next-day
                                                     :dist-day dist-day
                                                     :dist-month dist-month})))
        ;; if we made no changes then we are done
        (if (identical? exec-time next-exec-time)
          next-exec-time
          (recur next-exec-time))))))

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
