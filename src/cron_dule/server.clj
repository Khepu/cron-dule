(ns cron-dule.server
  (:require
   [aleph.http :as http]
   [reitit.ring :as ring]
   [clojure.data.json :as json]
   [ring.middleware.params :refer [wrap-params]]))


(def api-server (atom nil))

(defn register-cron [request]
  (let [body-params (json/read-str (slurp (:body request)) :key-fn keyword)]
    {:status 200
     :body (json/write-str {:data (+ 1 (get body-params :data -1))})}))

(def routes
  [["/ping"
    {:get {:handler (fn [req] {:status 200})}}]
   ["/cron"
    {:post {:handler #'register-cron}}]])

(def app
  (-> routes
      (ring/router {:data {:middleware [wrap-params]}})
      (ring/ring-handler (ring/create-default-handler) {})))

(defn start-server []
  (when-not @api-server
    (reset! api-server
            (http/start-server #'app {:port 8080}))))

(defn stop-server []
  (when @api-server
    (.close @api-server)
    (reset! api-server nil)))
