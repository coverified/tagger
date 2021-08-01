# Tagger

A microservice for entry tagging. This service queries existing entries from a database using graphQL, sends parts of their content for multi-label text classification to an AI REST api, analyzes the language of the entry content and afterwards persists the updated entry with the its labels and language in the database using the graphQL api. 

## Used Frameworks / Libraries
_(not comprehensive, but the most important ones)_

-   [akka](https://akka.io/)
-   [Caliban Client](https://ghostdogpr.github.io/caliban/) to talk to GraphQL endpoint
-   [Sentry](https://sentry.io/welcome/) (error reporting)

## Configuration
Configuration is done using environment variables. 
The following configuration parameters are available.

Environment config values:

```
NO_OF_CONCURRENT_WORKER = "10" (default: 10)
BATCH_SIZE = "100" (default: 10)
GRAPHQL_API_URL = "https://"
AUTH_SECRET = "YOUR-GRAPHQL-SECRET"
KI_API_URL = "https://"
INTERNAL_SCHEDULER_INTERVAL = "10" (default: -1 (=disabled))
BATCH_DELAY = delay time between batches, disabled by default
```
