import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta

TIME_FORMAT = "%Y/%m/%d %H:%M:%S"


@dataclass
class KdvhTable:
    duration: timedelta
    n_elements: int
    n_stations: int


@dataclass
class Args:
    db: str
    log_path: str
    dump_path: str
    table: str | None


def get_element_number(filename: str) -> int:
    try:
        with open(filename) as file:
            line_count = len(file.readlines())
    except:
        line_count = 0

    return line_count


def get_station_number(table_dir: str) -> int:
    with os.scandir(table_dir) as tdir:
        n_stations = 0
        for station in tdir:
            if not station.is_dir():
                continue
            if len(os.listdir(station.path)) == 0:
                continue
            n_stations += 1

    return n_stations


def print_kdvh_report(tables: dict[str, KdvhTable]):
    sorted_durations = {
        k: v for k, v in sorted(tables.items(), key=lambda item: item[1].duration)
    }

    # print in markdown table format
    for k, v in sorted_durations.items():
        print(f"| {k} | {v.n_stations} | {v.n_elements} | {v.duration} |")


def main(args: type[Args]):
    tables: dict[str, KdvhTable] = {}

    with os.scandir(args.log_path) as dir:
        for entry in dir:
            if not entry.name.endswith(".log"):
                continue

            table_name = entry.name.split("_dump")[0]
            if args.table is not None and args.table != table_name:
                continue

            with open(f"{args.log_path}/{entry.name}") as file:
                lines = file.readlines()
                first = " ".join(lines[0].split()[0:2])
                last = " ".join(lines[-1].split()[0:2])

            start = datetime.strptime(first, TIME_FORMAT)
            end = datetime.strptime(last, TIME_FORMAT)

            match args.db:
                case "kdvh":
                    table_dir = f"{args.dump_path}/kdvh/{table_name}_combined"
                    n_elements = get_element_number(f"{table_dir}/elements.txt")
                    n_stations = get_station_number(table_dir)

                    tables[table_name] = KdvhTable(end - start, n_elements, n_stations)

                case "kvalobs" | "histkvalobs":
                    pass
                case _:
                    pass

    match args.db:
        case "kdvh":
            print_kdvh_report(tables)
        case _:
            pass


def strip_ending_slash(input: str) -> str:
    if input.endswith("/"):
        return input[:-1]
    return input


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    _ = ap.add_argument(
        "db",
        type=str,
        default="kdvh",
        choices=("kdvh", "kvalobs", "histkvalobs"),
    )
    _ = ap.add_argument("-l", "--log-dir", dest="log_path", type=str, default=".")
    _ = ap.add_argument("-d", "--dump-dir", dest="dump_path", type=str, default="dumps")
    _ = ap.add_argument("-t", "--table", dest="table", type=str, default=None)

    args = ap.parse_args(namespace=Args)
    args.log_path = strip_ending_slash(args.log_path)
    args.dump_path = strip_ending_slash(args.dump_path)
    main(args)
