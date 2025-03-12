# Lard (**L**ive **A**tmospheric **R**eadings **D**atabase)

An ingestion, storage, and delivery system for meteorological observations that delivery reliability and performance, while remaining simple to understand, operate, and maintain.

TODO: Banner image?

## Status

Approaching beta, targeting summer 2025.

## Architecture

Lard is built around a Postgres database with two services that interact with it, one focused on ingestion, and one providing an API to access the data.

![Diagram of the architecture on a single node](https://raw.githubusercontent.com/metno/lard/872068b5530c64308908f99c6ab51ac85ca6a13f/docs/images/single-arch.svg)

This architecture lets it scale down to run on a single machine, while also scaling up to respond to high query volume:

TODO: Replicated architecture diagram

Here, one node takes responsiblity for ingestion, using [Postgres replication](https://www.postgresql.org/docs/current/high-availability.html) to sync the others. Meanwhile, the others focus on serving read-only requests from the API service, allowing read throughput to scale linearly with the number of replicas. Replicas are also able to take over from the primary in case of outages, minimising downtime.

In addition to read throughput, previous experience with database systems at Met has taught us that as our dataset grows (think past 1 billion observations) write throughput begins to slow to a problematic degree. This happens because the [indexes](https://www.postgresql.org/docs/current/indexes.html) (structures needed speed up queries on large tables) become resource intensive to maintain as they grow larger. Particularly the BTree indices we use to represent time need to remain a balanced, but as we always add data on one side of the tree (the present is one extreme of the time range our dataset covers), we are constantly unbalancing it, and the expense of balancing a tree scales with its size.

TODO: Tree balancing diagram

We've gotten around this by [partitioning](https://www.postgresql.org/docs/17/ddl-partitioning.html) the main data table in time, breaking up the indices, while still maintaining a single logical table from the perspective of the services.

TODO: Partitioning diagram

Deeper dives into the architecture of the components:

TODO: Link db architecture
TODO: Link ingestion architecture
TODO: Link API architecture

## Development

TODO:
