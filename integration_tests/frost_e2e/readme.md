### run:
*Need to be on MET network since rely on connection to stinfosys for egress, as well as login.*

just _setup_frost_e2e

But need the secrets (STINFO_CONN_STRING, JWKS_URL) that are not checked into the repo from:

https://gitlab.met.no/met/obsklim/bakkeobservasjoner/lagring-og-distribusjon/db-products/poda/-/settings/ci_cd#js-cicd-variables-settings

#### can check:
docker logs lard_egress

docker logs frost


### test:
See that patchwork has a timeseries:

http://localhost:3000/patchwork/available

See that we can get that timeseries and data out of frost:

http://localhost:8080/api/v1/obs/lardranked/get?stationids=18700&elementids=air_temperature&time=latest&incobs=true

(with the one user that is in the docker compose authdb: a8adfa00-6680-49b3-bf94-caa8c3f1d823)