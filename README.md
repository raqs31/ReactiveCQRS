### ReactiveCQRS
CQRS + Event Sourcing framework for Scala, built on Apache Pekko actors and PostgreSQL, for building reactive distributed applications.

**Documentation:** [docs/](docs/README.md) — [getting started](docs/getting-started.md) · [API](docs/api.md) · [core architecture](docs/core.md) · [configuration](docs/configuration.md) · [examples](docs/examples.md)

##### it uses Default singleton ScalikeJDBC connection pool, so it has to be initialized first


### TODO
- Asunchronous command handlers- Pass execution context to command handlers?
- Backpressure for projection rebuild
- Externalize datastore
- Non persistent projections

### TODO
- Projection rebuild
- Event bus database writes optimization - aggregate update in chunks
- Handle OptimisticLockingFailed
- Common transaction for document stores
- Caching document store - to improve performance of projection rebuilding
- document store based on scalikejdbc - to improve logging of queries
- verify and optimize db indieces
- clock injected to framework
- Query for events and aggregate state
