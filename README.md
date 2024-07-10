## Issue Flowchart

```mermaid
%%{ init : { "theme" : "default" }}%%
flowchart TD
  subgraph main[ ]
    subgraph lard[LARD in production]
      psa([Problem space analysis]):::done
      arc([Architecture of LARD]):::done
      test([DB test with fake data]):::done
      poc([API PoC]):::done
      frostb1(Frostv1 ObsBackend PoC):::done
      ingestkldata(Ingestion System - Kldata):::done

      migrate(Migration):::wip
      depl(Deployment playbook):::wip

      ingestbufr(Ingestion System - BUFR):::backlog
      ingestchecked(Ingestion System - Checked):::done
      products(Products layer):::wip
      frostb2(Frostv1 ObsBackend Prod-ready):::backlog
      beta(Beta testing):::backlog

      psa --> arc
      arc --> test
      test --> poc & ingestkldata & migrate & depl
      poc --> frostb1
      frostb1 --> products
      ingestkldata --> ingestbufr & ingestchecked
      products & ingestchecked --> frostb2
      ingestbufr & frostb2 & migrate & depl --> beta
    end

    subgraph fut[Future Work]
      rove[ROVE integration]:::backlog
      next[Next-gen API]:::backlog
      archive[Archiving]:::backlog
      obsinn[Obsinn revision]:::backlog
      pipe[New pipeline components]:::backlog
      iot[IoT DB]:::backlog
      radsat[Radar, satellite pipelines]:::backlog
    end

    poc --> next
    migrate & ingestkldata --> archive
    beta --> obsinn & pipe
    ingestkldata --> rove
  end

  classDef done fill:#c1fcbd,stroke:green
  classDef wip fill:#fdfaae,stroke:orange
  classDef backlog fill:#ffcad4,stroke:red
  classDef bkg fill:white,stroke:white
  classDef subBkg fill:#e8ecfc,stroke:#6483fc
  class main bkg
  class lard subBkg
  class fut subBkg
```

### Prospective Ranked Priority of Future Work:

- Archiving
- Obsinn revision
- Next-gen API
- New pipeline components
- Radar, satellite pipeline
- IoT DB
