# telemere.axiom

[![Clojars Project](https://img.shields.io/clojars/v/com.github.marksto/telemere.axiom.svg)](https://clojars.org/com.github.marksto/telemere.axiom)

A set of things necessary for [Telemere](https://github.com/taoensso/telemere)-to-[Axiom.co](https://axiom.co) integration.

Defines and operates through a configurable [signal handler](https://github.com/taoensso/telemere/wiki/4-Handlers) that prepares signals and periodically sends them in batches to the [Ingest API](https://axiom.co/docs/send-data/ingest#ingest-api).

## Usage

Have `com.taoensso/telemere` on your classpath. The minimum supported version of Telemere is `1.0.0-RC1`.

Add `com.github.marksto/telemere.axiom` to your project dependencies.

```clojure
(require '[marksto.telemere.axiom :as mta])
(require '[taoensso.telemere :as tel])

(def handler-opts
  {:conn-opts {:api-token <AXIOM_API_TOKEN>
               :dataset   <AXIOM_DATASET>}})

(def handler-fn (mta/handler:axiom handler-opts))

(tel/add-handler! :axiom handler-fn)
; or
(tel/add-handler! :axiom handler-fn <dispatch-opts>)
```

Here `<AXIOM_API_TOKEN>` and `<AXIOM_DATASET>` are the values you configured in Axiom. For more details, see the Axiom [Settings](https://axiom.co/docs/reference/settings) documentation.

## Documentation

Please see the docstring of the `handler:axiom` function.

## License

Licensed under [EPL 1.0](LICENSE) (same as Clojure).
