;; Copyright (c) Mark Sto, 2025. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file `LICENSE` at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by the
;; terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns marksto.telemere.axiom
  "A set of things necessary for Telemere-to-Axiom.co integration.

   Telemere: https://github.com/taoensso/telemere
   Axiom.co: https://axiom.co"
 {:author "Mark Sto (@marksto)"}
 (:require #?(:clj  [clj-http.client :as http]
              :cljs ["node-fetch" :as fetch])
           [clojure.pprint :as pp]
           [clojure.set :as set]
           #?(:cljs [clojure.string :as str])
           #?(:clj  [clojure.stacktrace :refer [print-cause-trace]])
           #?(:clj  [jsonista.core :as json]
              :cljs [clojure.walk :as walk])
           [taoensso.telemere.utils]
           [taoensso.telemere])
 #?(:clj (:import (java.time Instant)
                  (java.util.concurrent Executors ScheduledExecutorService TimeUnit))))

;; TODO: Account for dynamic rate limits?
;;       https://axiom.co/docs/restapi/api-limits#ingest-limits

;; TODO: Support sending data using OpenTelemetry?
;;       https://axiom.co/docs/send-data/opentelemetry


#?(:clj (def default-mapper
          (json/object-mapper {:date-format   "yyyy-MM-dd'T'HH:mm:ssXXX"
                               :decode-key-fn true})))

#?(:cljs (defn encode-date [d]
           (.toISOString d))) ;; ISO 8601 format


#?(:cljs (defn encode-dates [data]
           (walk/postwalk
            (fn [x]
              (cond
                (instance? js/Date x) (encode-date x)
                :else x))
            data)))


(defn base-request [api-token]
  (merge {:headers {"Authorization" (str "Bearer " api-token)
                    "Content-Type" "application/json"}}
         #?(:clj  {:async?       true
                   :content-type :json
                   :accept       :json}
            :cljs {:method "POST"})))

(defn build-request
  [base-req signals]
  #?(:clj  (assoc base-req :body (json/write-value-as-string signals default-mapper))
     :cljs (assoc base-req :body (js/JSON.stringify (clj->js (encode-dates signals))))))

(defn ->send!
  [{:keys [api-token dataset] :as _conn-opts}]
  (let [api-url (str "https://api.axiom.co/v1/datasets/" dataset "/ingest")
        base-req (base-request api-token)]
    (fn [signals & [ok-fn err-fn]]
      (let [ok-fn (or ok-fn (constantly nil))
            err-fn (or err-fn (constantly nil))]
        #?(:clj  (let [req (build-request base-req signals)]
                   (http/post api-url req #(ok-fn (:body %)) err-fn))
           :cljs (let [req (clj->js (build-request base-req signals))]
                   (-> (fetch api-url req)
                       (.then (fn [resp]
                                (-> (.json resp)
                                    (.then (fn [d]
                                             (let [data (js->clj d :keywordize-keys true)]
                                               (if-not (some? (:code data))
                                                 (ok-fn data)
                                                 (err-fn data))))))))
                       (.catch (fn [err]
                                 (err-fn err))))))))))


(def default-clean-signal-fn
  taoensso.telemere.utils/clean-signal-fn)

(def renames {:inst :_time
              :msg_ :msg})

(defn default-prepare-signal-fn
  [& [{:keys [stringify-data-field?] :or {stringify-data-field? true}}]]
  (fn default-prepare-signal
    [{:keys [data] :as signal}]
    (-> signal
        ((default-clean-signal-fn))
        (update :msg_ force)
        (set/rename-keys renames)
        (cond->
         (and (some? data) stringify-data-field?)
          (update :data #(with-out-str (pp/pprint %)))))))

#?(:cljs (defn print-cause-trace
           "Prints a stack trace for an exception, including its cause chain, similar to
            clojure.stacktrace/print-cause-trace in Clojure."
           ([ex] (print-cause-trace ex 0))
           ([ex depth]
            (when ex
              (let [indent (str/join (repeat (* depth 2) " "))
                    stack (or (.-stack ex) "No stack trace available")]
                (println (str indent "Exception: " (pr-str ex)))
                (println (str indent "Stack trace:"))
                (println (str indent stack))
                (when-let [cause (.-cause ex)]
                  (println (str indent "Caused by:"))
                  (print-cause-trace cause (inc depth))))))))

(defn default-handle-ex
  [phase #?(:clj ^Throwable err :cljs ^js/Error err) arg]
  (let [msg (case phase
              :prepare-signal "Error while preparing a signal"
              :process-batch "Error while processing signals batch")]
    (binding [*out* #?(:clj *err* :cljs *out*)]
      (print-cause-trace (ex-info msg {:arg arg} err))
      (flush))))

(defn create-batch-processor!
  [process-batch-fn ex-handler rate-ms]
  (let [*signals (atom [])
        #?(:clj executor :cljs !interval-id) #?(:clj (Executors/newSingleThreadScheduledExecutor) :cljs (atom nil))
        activity (fn []
                   (let [[signals] (reset-vals! *signals [])]
                     (when (seq signals)
                       (try
                         (process-batch-fn signals)
                         (catch #?(:clj Throwable :cljs js/Error) t
                           (ex-handler :process-batch t signals))))))]
    #?(:clj (ScheduledExecutorService/.scheduleAtFixedRate
             executor activity rate-ms rate-ms TimeUnit/MILLISECONDS)
       :cljs (js/setTimeout
              #(reset! !interval-id (js/setInterval activity rate-ms))
              rate-ms))
    {:add!  (fn add-to-batch [signal]
              (swap! *signals conj signal)
              true)
     :stop! (fn []
              #?(:clj  (ScheduledExecutorService/.shutdown executor)
                 :cljs (js/clearInterval @!interval-id)))}))

(defn test-signal []
  {:_time #?(:clj (Instant/now)
             :cljs (js/Date.))
   :level :info
   :kind  :log
   :ns    "marksto.telemere.axiom"
   :msg   "Axiom handler is added"
   :ctx   #?(:clj @(requiring-resolve 'taoensso.telemere/*ctx*)
             :cljs taoensso.telemere/*ctx*)})

(defn val+type [obj]
  {:value obj
   :type  (type obj)})

(defn validate-constructor-opts!
  [{{:keys [api-token dataset]} :conn-opts :as constructor-opts}]
  (when-not (string? api-token)
    (throw
     (ex-info "Expected `:conn-opts/api-token` string" (val+type api-token))))
  (when-not (string? dataset)
    (throw
     (ex-info "Expected `:conn-opts/dataset` string" (val+type dataset))))
  (let [rate-ms (get constructor-opts :rate-ms ::default)]
    (when-not (or (identical? ::default rate-ms) (pos-int? rate-ms))
      (throw
       (ex-info "Expected `:rate-ms` positive integer" (val+type rate-ms))))))

(defn handler:axiom
  "Builds a stateful signal handler that sends all signals to Axiom Ingest API.

   Keys of the `constructor-opts` map:
   - `:conn-opts`  — a map with mandatory `:api-token` and `:dataset` keys that
                     are used to establish a connection with the Axiom via API;
   - `:rate-ms`    — a positive int that sets the period in millis at which all
                     received signals are prepared and sent in batches; default
                     is 3000 (3 seconds);
   - `:prepare-fn` — a unary fn that modifies every signal prior to sending it;
                     the default impl applies the Telemere's `clean-signal-fn`,
                     renames the `:inst` key to `:_time` and the `:msg_` key to
                     `:msg`, and stringifies the `:data` value, if any;
   - `:ex-handler` — a ternary fn of `phase` #{:prepare-signal :process-batch},
                     Throwable and `arg` (a single signal or vector of signals,
                     depending on the `phase`) that handles an exception/error;
                     the default impl simply prints out an exception.
   - `:debug?`     — a boolean that enables debug logging.

   Returns a handler function."
  [{:keys [conn-opts rate-ms prepare-fn ex-handler debug?]}]
  (let [constructor-opts {:conn-opts  (or conn-opts {:api-token nil
                                                     :dataset   nil})
                          :rate-ms    (or rate-ms 300)
                          :prepare-fn (or prepare-fn (default-prepare-signal-fn))
                          :ex-handler (or ex-handler default-handle-ex)
                          :debug?     (or debug? false)}]
    (validate-constructor-opts! constructor-opts)
    (let [{:keys [conn-opts rate-ms prepare-fn ex-handler debug?]} constructor-opts
          send! (->send! conn-opts)
          _ (try (send! [(test-signal)]
                        (fn [resp]
                          (println "Axiom ingested a test signal" resp))
                        (fn [data]
                          (if (instance? #?(:clj Throwable :cljs js/Error) data)
                            (throw data)
                            (throw (ex-info "Problem on ingesting a test signal" {:data data})))))
                 (catch #?(:clj Exception :cljs js/Error) ex
                   (throw (ex-info "Axiom failed to ingest a test signal" (ex-data ex) ex))))

          prepare+send! (fn prepare+send! [signals]
                          (let [prepared-signals (->> signals
                                                      (map #(try
                                                              (prepare-fn %)
                                                              (catch #?(:clj Throwable :cljs js/Error) t
                                                                (ex-handler :prepare-signal t %)
                                                                nil)))
                                                      (remove nil?))]
                            (send! prepared-signals
                                   (fn [body]
                                     (when debug?
                                       (println "[DEBUG] (telemere.axiom) Axiom ingested batch:" body)))
                                   (fn [err]
                                     (when debug?
                                       (println "[DEBUG] (telemere.axiom) Axiom batch ingest failed:" err))))))

          {:keys [add! stop!]}
          (create-batch-processor! prepare+send! ex-handler rate-ms)]
      (with-meta
        (fn a-handler:axiom
          ([]
           (stop!))
          ([signal]
           (add! signal)))
        {:dispatch-opts {:async {}}}))))