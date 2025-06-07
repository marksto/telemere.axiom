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
  (:require [clj-http.client :as http]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.stacktrace :refer [print-cause-trace]]
            [jsonista.core :as json])
  (:import (java.time Instant)
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit)))

;; TODO: Account for dynamic rate limits?
;;       https://axiom.co/docs/restapi/api-limits#ingest-limits

;; TODO: Support sending data using OpenTelemetry?
;;       https://axiom.co/docs/send-data/opentelemetry

(def axiom-date-format "yyyy-MM-dd'T'HH:mm:ssXXX")

(def default-obj-mapper
  (json/object-mapper {:date-format   axiom-date-format
                       :decode-key-fn true}))

(defn base-request [api-token]
  {:content-type :json
   :accept       :json
   :headers      {"Authorization" (str "Bearer " api-token)}})

(defn build-request
  [base-req obj-mapper signals]
  (assoc base-req :body (json/write-value-as-string signals obj-mapper)))

(defn build-send!
  [{:keys [api-token dataset] :as _conn-opts} obj-mapper]
  (let [api-url (format "https://api.axiom.co/v1/datasets/%s/ingest" dataset)
        base-req (base-request api-token)]
    (fn [ret-resp? signals]
      (let [req (build-request base-req obj-mapper signals)
            resp (http/post api-url req)]
        (when ret-resp?
          (update resp :body #(json/read-value % obj-mapper)))))))

(def *default-clean-signal-fn
  (delay (requiring-resolve 'taoensso.telemere.utils/clean-signal-fn)))

(def renames {:inst :_time
              :msg_ :msg})

(defn build-prepare-fn
  "Builds a unary fn for preparing an individual signal.

   Optional parameters:
   - `:clean-signal?`     — if `true` will call the Telemere's built-in
                            utility for cleaning the signal from noise;
   - `:clean-signal-opts` — a map of options passed to `clean-signal-fn`
                            in case the `:clean-signal?` is `true`;
   - `:data-update-fn`    — a unary fn for updating the signal's `:data`
                            value, e.g. stringifying/prettifying it."
  [{:keys [clean-signal? clean-signal-opts data-update-fn]}]
  (let [clean-signal (if clean-signal?
                       (@*default-clean-signal-fn clean-signal-opts)
                       identity)
        prepare-data (if data-update-fn
                       (fn [{:keys [data] :as signal}]
                         (if (some? data)
                           (update signal :data data-update-fn)
                           signal))
                       identity)]
    (fn [signal]
      (-> signal
          (clean-signal)
          (update :msg_ force)
          (set/rename-keys renames)
          (prepare-data)))))

(def default-prepare-fn
  (build-prepare-fn {:clean-signal?  true
                     :data-update-fn #(with-out-str (pp/pprint %))}))

(defn print-ex-to-stderr
  [phase ^Throwable t arg]
  (let [msg (case phase
              :prepare-signal "Error while preparing a signal"
              :process-batch "Error while processing signals batch")]
    (binding [*out* *err*]
      (print-cause-trace (ex-info msg {:arg arg} t))
      (flush))))

(defn create-batch-processor!
  [process-batch-fn ex-handler rate-ms]
  (let [*signals (atom [])
        executor (Executors/newSingleThreadScheduledExecutor)
        activity (fn []
                   (let [[signals] (reset-vals! *signals [])]
                     (when (seq signals)
                       (try
                         (process-batch-fn signals)
                         (catch Throwable t
                           (ex-handler :process-batch t signals))))))]
    (ScheduledExecutorService/.scheduleAtFixedRate
      executor activity rate-ms rate-ms TimeUnit/MILLISECONDS)
    {:add!  (fn add-to-batch [signal]
              (swap! *signals conj signal)
              true)
     :stop! (fn []
              (ScheduledExecutorService/.shutdown executor))}))

(defn test-signal []
  {:_time (Instant/now)
   :level :info
   :kind  :log
   :ns    "marksto.telemere.axiom"
   :msg   "Axiom handler is added"
   :ctx   @(requiring-resolve 'taoensso.telemere/*ctx*)})

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
                     `:msg`, and stringifies the `:data` value, if there's any;
                     see the `build-prepare-fn`;
   - `:obj-mapper` — an `ObjectMapper` for JSON encoding/decoding both requests
                     and response bodies; uses `default-obj-mapper` by default;
   - `:ex-handler` — a ternary fn of `phase` #{:prepare-signal :process-batch},
                     Throwable and `arg` (a single signal or vector of signals,
                     depending on the `phase`) that handles an exception/error;
                     the default impl simply prints out an exception.

   Returns a handler function."
  [{:keys [conn-opts rate-ms prepare-fn obj-mapper ex-handler]
    :or   {conn-opts  {:api-token nil
                       :dataset   nil}
           rate-ms    3000
           prepare-fn default-prepare-fn
           obj-mapper default-obj-mapper
           ex-handler print-ex-to-stderr}
    :as   constructor-opts}]
  (validate-constructor-opts! constructor-opts)
  (let [send! (build-send! conn-opts obj-mapper)
        _ (try
            (send! true [(test-signal)])
            (catch Exception ex
              (throw (ex-info "Axiom failed to ingest a test signal" {} ex))))

        prepare+send! (fn prepare+send! [signals]
                        (->> signals
                             (map #(try
                                     (prepare-fn %)
                                     (catch Throwable t
                                       (ex-handler :prepare-signal t %)
                                       nil)))
                             (remove nil?)
                             (send! false)))

        {:keys [add! stop!]}
        (create-batch-processor! prepare+send! ex-handler rate-ms)]
    (with-meta
      (fn a-handler:axiom
        ([]
         (stop!))
        ([signal]
         (add! signal)))
      {:dispatch-opts {:async nil}})))
