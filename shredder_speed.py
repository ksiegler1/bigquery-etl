#!/usr/bin/env python

from argparse import ArgumentParser
from datetime import datetime, timedelta
from itertools import groupby
from multiprocessing.pool import ThreadPool
import warnings

from google.cloud import bigquery

from bigquery_etl.shredder.config import DELETE_TARGETS
from bigquery_etl.util.bigquery_id import FULL_JOB_ID_RE, sql_table_id


def mean(iterable):
    """Calculated the mean of an iterable of optional values.

    Each item in iterable may be a value with an implicit weight of 1 or a
    tuple of value and weight. When value is None exclude it from the mean.
    """
    total_value = total_weight = 0
    for item in iterable:
        if isinstance(item, tuple):
            value, weight = item
        else:
            value, weight = item, 1
        if value is None:
            continue
        total_value += value * weight
        total_weight += weight
    if total_weight != 0:
        return total_value / total_weight


def add_argument(parser, *args, **kwargs):
    if "default" in kwargs and "help" in kwargs:
        kwargs["help"] += f"; default {kwargs['default']!r}"
    parser.add_argument(*args, **kwargs)


parser = ArgumentParser()
add_argument(
    parser,
    "-s",
    "--slots",
    default=1000,
    type=int,
    help="Number of reserved slots currently available",
)
add_argument(
    parser,
    "-S",
    "--state-table",
    "--state_table",
    default="moz-fx-data-shredder.shredder_state.shredder_state",
    help="Table used to store shredder state for script/shredder_delete",
)
add_argument(
    parser,
    "-d",
    "--days",
    default=28,
    type=int,
    help="Number of days shredder has to run",
)
add_argument(
    parser,
    "-P",
    "--parallelism",
    default=20,
    type=int,
    help="Number of threads to use when listing jobs and tables",
)


args = parser.parse_args()
flat_rate_slots = args.slots
runs_per_month = 365 / 12 / args.days

# translate days per run to seconds per run
seconds_per_day = 60 * 60 * 24
seconds_per_run = args.days * seconds_per_day

# calculate the minimum bytes_per_second a job needs to process to reduce cost
# by using flat rate pricing instead of on demand pricing at 100% utilization
seconds_per_month = seconds_per_day * 7 * 52 / 12
flat_rate_dollars_per_month_per_slot = 8500 / 500
on_demand_bytes_per_dollar = 2 ** 40 / 5
min_flat_rate_bytes_per_second_per_slot = (
    on_demand_bytes_per_dollar
    * flat_rate_dollars_per_month_per_slot
    / seconds_per_month
)

# translate min_flat_rate_bytes_per_second_per_slot to slot_millis_per_byte
slot_millis_per_second_per_slot = 1000
min_flat_rate_slot_millis_per_byte = (
    slot_millis_per_second_per_slot / min_flat_rate_bytes_per_second_per_slot
)


def get_bytes_per_second(jobs, table=None, verbose=True):
    if table is not None:
        table_id = sql_table_id(table)
        jobs = [job for job in jobs if sql_table_id(job.destination) == table_id]
    if jobs:
        total_bytes_processed, slot_millis = map(
            sum, zip(*((job.total_bytes_processed, job.slot_millis) for job in jobs))
        )
        if total_bytes_processed == 0 or slot_millis == 0:
            return None
        slot_millis_per_byte = slot_millis / total_bytes_processed
        bytes_per_second = total_bytes_processed / (
            slot_millis / slot_millis_per_second_per_slot / flat_rate_slots
        )
        if verbose:
            print(
                f"shredder is processing {table.table_id + ' at ' if table else ''}{bytes_per_second*60/2**30:.3f} GiB/min using {flat_rate_slots} slots"
            )
            # report cost vs on-demand
            efficiency = slot_millis_per_byte / min_flat_rate_slot_millis_per_byte
            tense = "cheaper" if efficiency <= 1 else "more expensive"
            efficiency_percent = abs(100 - efficiency * 100)
            print(
                f"processing speed{' for ' + table.table_id if table else ''} is "
                f"{efficiency_percent:.2f}% {tense} than on-demand at 100% utilization"
            )
        return bytes_per_second
    return None


# determine how fast shredder is running
warnings.filterwarnings("ignore", module="google.auth._default")
client = bigquery.Client()

with ThreadPool(args.parallelism) as pool:
    jobs = pool.starmap(
        client.get_job,
        [
            (job["job_id"], job["project"], job["location"])
            for row in client.query(
                # most recent job for each task_id and end_date
                "SELECT ARRAY_AGG(job_id ORDER BY job_created DESC)[OFFSET(0)] "
                f"FROM `{args.state_table}` "
                "GROUP BY task_id"
            )
            for job in [FULL_JOB_ID_RE.match(row[0])]
            if job["project"] == "moz-fx-data-bq-batch-prod"
        ],
    )
    jobs = [job for job in jobs if job.state == "DONE" and not job.errors]

    tables = pool.map(
        client.get_table, [sql_table_id(target) for target in DELETE_TARGETS]
    )
    flat_rate_tables = [table for table in tables if table.table_id != "main_v4"]
    on_demand_tables = [table for table in tables if table.table_id == "main_v4"]

bytes_per_second = get_bytes_per_second(jobs)

# report how long it would take to process specific tables
only_tables = {"main_summary_v4"}
for table in tables:
    if table.table_id not in only_tables:
        continue
    table_bytes_per_second = get_bytes_per_second(jobs, table)
    if table_bytes_per_second is None:
        print(f"no jobs completed yet for {table.table_id}, skipping time estimate")
    else:
        print(
            f"{timedelta(seconds=(table.num_bytes)/table_bytes_per_second)} to process "
            f"{table.table_id} with {flat_rate_slots} slots"
        )

# estimate resources needed to run shredder
# report how many slots it would take to process flat-rate tables
flat_rate_speeds_and_bytes = [
    (get_bytes_per_second(jobs, table, verbose=False), table.num_bytes)
    for table in flat_rate_tables
]
# mean weighted by table.num_bytes for tables with jobs
mean_bytes_per_second = mean(flat_rate_speeds_and_bytes)
# calculate using mean_bytes_per_second for tables with no jobs
slots_needed = sum(
    num_bytes
    / seconds_per_run
    / (table_bytes_per_second or mean_bytes_per_second)
    * flat_rate_slots
    for table_bytes_per_second, num_bytes in flat_rate_speeds_and_bytes
)
num_bytes = sum(table.num_bytes for table in flat_rate_tables)
# report slots needed to process flat-rate tables
print(
    f"{slots_needed:.0f} slots needed to process {num_bytes/2**50:.3f} PiB every "
    f"{args.days} day(s) for everything except main_v4"
)

# report how much it would cost to process on-demand tables
num_bytes = sum(table.num_bytes for table in on_demand_tables)
dollars_per_month = num_bytes * runs_per_month / on_demand_bytes_per_dollar
print(
    f"${dollars_per_month:,.2f}/mo to process {num_bytes/2**50:.3f} PiB every "
    f"{args.days} day(s) on-demand for main_v4"
)
