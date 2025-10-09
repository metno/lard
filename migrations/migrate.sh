dump_dir=$1
max_obstime=$2

go build

./migrate index drop
./migrate kdvh import -p "$dump_dir"/kdvh
./migrate kvalobs import -p "$dump_dir"/histkvalobs
./migrate kvalobs import -p "$dump_dir"/kvalobs
./migrate index create
./migrate lard update "$max_obstime"
