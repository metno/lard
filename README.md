## Issue Flowchart

```mermaid
flowchart TD
    subgraph lard[LARD in production]
        psa([Problem space analysis]):::done
        arc([Architecture of LARD]):::done
        test([DB test with fake data]):::done
        poc([API PoC]):::done

        frostb(Frost Backend):::wip
        ingest(Ingestion System):::wip
        migrate(Migration):::wip
        depl(Deployment playbook):::wip

        products[Products layer]:::backlog
        rove[ROVE integration]:::backlog
        beta[Beta testing]:::backlog

        psa --> arc
        arc --> test
        test --> poc & ingest & migrate & depl
        poc --> frostb
        frostb --> products
        ingest --> rove
        products & rove & migrate & depl --> beta
    end
    subgraph "Future Work"
        next[Next-gen API]:::backlog
        archive[Archiving]:::backlog
        obsinn[Obsinn revision]:::backlog
        pipe[New pipeline components]:::backlog
        iot[IoT DB]:::backlog
        radsat[Radar, satellite pipelines]:::backlog
    end
    poc --> next
    migrate & ingest --> archive
    beta --> obsinn & pipe

    classDef done fill:#d8e2dc
    classDef wip fill:#ffe5d9
    classDef backlog fill:#ffcad4
```

### Prospective Ranked Priority of Future Work:
- Archiving
- Obsinn revision
- Next-gen API
- New pipeline components
- Radar, satellite pipeline
- IoT DB
