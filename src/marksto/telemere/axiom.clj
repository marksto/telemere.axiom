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
            [jsonista.core :as json]
            [marksto.telemere.utils :as utils])
  (:import (java.time Instant)
           (java.util.concurrent ConcurrentLinkedDeque
                                 Executors
                                 ScheduledExecutorService
                                 TimeUnit)))

;; TODO: Account for other ingestion limits?
;;       - data volume limits via the 'X-IngestLimit-***' headers
;;         https://axiom.co/docs/restapi/api-limits#ingest-limits
;;       - "Maximum event size" and "Maximum field name length"
;;         https://axiom.co/docs/restapi/api-limits#limits-on-ingested-data

;; TODO: Support 'gzip'-encoding of requests?
;;       https://github.com/axiomhq/axiom-js/blob/1d0de5fc52c2c8820dfbfc1827b673be36136089/packages/js/src/client.ts#L34

;; TODO: Support sending data using OpenTelemetry?
;;       https://axiom.co/docs/send-data/opentelemetry

(def axiom-api-url "https://api.axiom.co")

(def axiom-date-format "yyyy-MM-dd'T'HH:mm:ssXXX")

(defn ->default-obj-mapper []
  (json/object-mapper {:date-format   axiom-date-format
                       :decode-key-fn true}))

(defn base-request [api-token org-id]
  {:content-type :json
   :accept       :json
   :headers      (cond-> {"Authorization" (str "Bearer " api-token)}
                         (some? org-id) (assoc "X-Axiom-Org-Id" org-id))})

(defn build-request
  [base-req obj-mapper signals]
  (assoc base-req :body (json/write-value-as-string signals obj-mapper)))

(defn build-send!
  [{:keys [api-url api-token org-id dataset] :as _conn-opts} obj-mapper]
  (let [endpoint (format "%s/v1/datasets/%s/ingest"
                         (or api-url axiom-api-url) dataset)
        base-req (base-request api-token org-id)]
    (fn [ret-resp? signals]
      (let [req (build-request base-req obj-mapper signals)
            resp (http/post endpoint req)]
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
                            value, e.g. stringifying/prettifying it.

   Always renames signal keys: `:inst` to `:_time` and `:msg_` to `:msg`."
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

(defn ->default-prepare-fn
  "Builds default `:prepare-fn` that cleans a signal by Telemere's built-in fn,
   renames keys, and also stringifies/prettifies the `:data` value, if there's
   any, so that it is an EDN-formatted 'stringType' in your Axiom's dataset."
  []
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

(def max-batch-size
  "Maximum events in a batch as per the \"Limits on ingested data\".
   https://axiom.co/docs/restapi/api-limits#limits-on-ingested-data"
  10000)

(defn shutdown-uninterruptedly!
  [executor period-ms]
  (ScheduledExecutorService/.shutdown executor)
  (utils/uninterruptibly
    (while (not (ScheduledExecutorService/.awaitTermination
                  executor (quot period-ms 10) TimeUnit/MILLISECONDS)))))

(defn collect-batch
  [all-sigs batch-size]
  (-> (loop [i 0, acc (transient [])]
        (if (< i batch-size)
          (if-some [signal (ConcurrentLinkedDeque/.pollFirst all-sigs)]
            (recur (inc i) (conj! acc signal))
            acc)
          acc))
      (persistent!)
      (not-empty)))

(defn restore-batch
  [all-sigs batch]
  (doseq [signal (rseq batch)]
    (ConcurrentLinkedDeque/.addFirst all-sigs signal)))

(defn create-batch-processor!
  [process-batch-fn ex-handler batch-size period-ms]
  (let [all-sigs (ConcurrentLinkedDeque.)
        executor (Executors/newSingleThreadScheduledExecutor)
        activity (fn [batch-size]
                   (when-some [batch (collect-batch all-sigs batch-size)]
                     (try
                       (process-batch-fn batch)
                       (catch Throwable t
                         (restore-batch all-sigs batch)
                         (ex-handler :process-batch t batch)))))]
    (ScheduledExecutorService/.scheduleAtFixedRate
      executor #(activity batch-size) period-ms period-ms TimeUnit/MILLISECONDS)
    {:add!  (fn add-to-batch [signal]
              (ConcurrentLinkedDeque/.offerLast all-sigs signal))
     :stop! (fn []
              (shutdown-uninterruptedly! executor period-ms)
              ;; Make sure all received signals get flushed!
              (activity max-batch-size))}))

(defn test-signal []
  {:_time (Instant/now)
   :level :info
   :kind  :log
   :ns    "marksto.telemere.axiom"
   :msg   "Axiom handler is added"
   :ctx   @(requiring-resolve 'taoensso.telemere/*ctx*)})

(defn validate-constructor-opts!
  [{{:keys [api-token dataset]} :conn-opts
    batch-size                  :batch-size
    period-ms                   :period-ms
    :as                         constructor-opts}]
  (when-not (string? api-token)
    (throw
      (ex-info "Expected `:conn-opts :api-token` string"
               (utils/value+type api-token))))
  (when-not (string? dataset)
    (throw
      (ex-info "Expected `:conn-opts :dataset` string"
               (utils/value+type dataset))))
  (when (and (contains? constructor-opts :batch-size)
             (not (pos-int? batch-size)))
    (throw
      (ex-info "Expected `:batch-size` positive integer"
               (utils/value+type batch-size))))
  (when (and (contains? constructor-opts :period-ms)
             (not (pos-int? period-ms)))
    (throw
      (ex-info "Expected `:period-ms` positive integer"
               (utils/value+type period-ms)))))

(defn handler:axiom
  "Builds a stateful signal handler that sends all signals to Axiom Ingest API.

   Keys of the `constructor-opts` map:
   - `:conn-opts`  — a map with all mandatory (`:api-token` and `:dataset`) and
                     some optional (`:api-url`, `:org-id`) keys, which are used
                     to establish a connection with the Axiom via REST API;
   - `:prepare-fn` — a unary fn that modifies every signal prior to sending it;
                     if not specified the default is used; build a custom value
                     with `build-prepare-fn`;
   - `:obj-mapper` — an `ObjectMapper` for JSON encoding/decoding both requests
                     and response bodies; if not specified the default is used;
   - `:ex-handler` — a ternary fn of `phase` #{:prepare-signal :process-batch},
                     Throwable and `arg` (a single signal or vector of signals,
                     depending on the `phase`) that handles an exception/error;
                     the default impl simply prints out an exception;
   - `:batch-size` — a positive int that limits the size of each signals batch;
                     defaults to the `max-batch-size`, the Axiom's limit;
   - `:period-ms`  — a positive int that sets the period in millis at which all
                     received signals are prepared and sent in batches; default
                     is 1000 (1 second) as in other backend Axiom client libs.

   Returns a handler function."
  [{:keys [conn-opts prepare-fn obj-mapper ex-handler batch-size period-ms]
    :or   {batch-size max-batch-size
           period-ms  1000}
    :as   constructor-opts}]
  (validate-constructor-opts! constructor-opts)
  (let [prepare-fn (or prepare-fn (->default-prepare-fn))
        obj-mapper (or obj-mapper (->default-obj-mapper))
        ex-handler (or ex-handler print-ex-to-stderr)

        send! (build-send! conn-opts obj-mapper)
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
        (create-batch-processor! prepare+send! ex-handler batch-size period-ms)]
    (with-meta
      (fn a-handler:axiom
        ([]
         (stop!))
        ([signal]
         (add! signal)))
      {:dispatch-opts {:async nil}})))
