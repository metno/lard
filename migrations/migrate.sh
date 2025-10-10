if [[ -z "${GOMEMLIMIT+x}" ]]; then
    echo "You need to set GOMEMLIMIT. For example:"
    echo "GOMEMLIMIT=6GiB bash migrate.sh dump_dir"
    exit 1
fi

dump_dir=$1

go build

./migrate index drop
./migrate kdvh import -p "$dump_dir"/kdvh
./migrate kvalobs import -p "$dump_dir"/histkvalobs
./migrate kvalobs import -p "$dump_dir"/kvalobs
./migrate index create
