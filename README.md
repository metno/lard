# Lard (**L**ive **A**tmospheric **R**eadings **D**atabase)

An ingestion, storage, and delivery system for meteorological observations that delivery reliability and performance, while remaining simple to understand, operate, and maintain.

TODO: Banner image?

## Status

Approaching beta, targeting summer 2025.

## Architecture

Lard is built around a Postgres database with two services that interact with it, one focused on ingestion, and one providing an API to access the data.

<!-- ![Diagram of the architecture on a single node](/docs/images/single-arch.svg) -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="/docs/images/single-arch-dark.svg">
  <!-- <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/GiorgosXou/Random-stuff/main/Programming/StackOverflow/Answers/70200610_11465149/b.png"> -->
  <img alt="Diagram of the architecture on a single node" src="/docs/images/single-arch.svg">
</picture>

This architecture lets it scale down to run on a single machine, while also scaling up to respond to high query volume:

<!-- ![Diagram of the architecture on a cluster of nodes](/docs/images/multi-arch.svg) -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="/docs/images/multi-arch-dark.svg">
  <!-- <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/GiorgosXou/Random-stuff/main/Programming/StackOverflow/Answers/70200610_11465149/b.png"> -->
  <img alt="Diagram of the architecture on a cluster of nodes" src="/docs/images/multi-arch.svg">
</picture>

Here, one node takes responsiblity for ingestion, using [Postgres replication](https://www.postgresql.org/docs/current/high-availability.html) to sync the others. Meanwhile, the others focus on serving read-only requests from the API service, allowing read throughput to scale linearly with the number of replicas. Replicas are also able to take over from the primary in case of outages, minimising downtime.

In addition to read throughput, previous experience with database systems at Met has taught us that as our dataset grows (think past 1 billion observations) write throughput begins to slow to a problematic degree. This happens because the [indexes](https://www.postgresql.org/docs/current/indexes.html) (structures needed to speed up queries on large tables) become resource intensive to maintain as they grow larger. Particularly the BTree indices we use to represent time need to remain balanced, but as we always add data on one side of the tree (the present is one extreme of the time range our dataset covers), we are constantly unbalancing it, and the expense of balancing a tree scales with its size.

We've gotten around this by [partitioning](https://www.postgresql.org/docs/17/ddl-partitioning.html) the main data table in time, breaking up the indices, while still maintaining a single logical table from the perspective of the services.

Deeper dives into the architecture of the components:

- [Database](/docs/DATABASE.md)
- [Ingestion](/docs/INGESTION.md)
- TODO: Link egress architecture
- [Integration tests](/docs/INTEGRATION_TESTS.md)

- TODO: Products
- TODO: QC

## Deployment

TODO: Publish container image?

At Met Norway we use [these ansible playbooks](/ansible) to manage a VM based deployment on our local [OpenStack](https://www.openstack.org/). These are somewhat specific to our infrastructure, but can serve as a good starting point for your own playbooks.

## Development

With [Rust](https://www.rust-lang.org/) installed, compile the project with:
```
cargo build --workspace
```

We have integration tests that require a local postgres instance to run. To save having to maintain a local postgres, we provide a [justfile](https://just.systems) that orchestrates setup and teardown in a container, and runs the tests with:
```
just test_all
```

This requires you to have [Docker](https://www.docker.com) (or an equivalent substitute) installed
