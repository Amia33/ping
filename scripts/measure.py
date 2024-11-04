"""Measure"""

from os import getenv
from pathlib import Path
from dotenv import load_dotenv
from modules import (
    sync_probe,
    create_measurement,
    get_measurement,
    connect,
    write_probe_id,
    write_measurement,
    write_result
)

if Path('scripts/.env').exists():
    load_dotenv('scripts/.env')
targets = getenv("TARGETS").split(",")
conn_str = getenv("CONN_STR")
api_key = getenv("API_KEY")
client = connect(conn_str)

sync_probe(client)
measurement_ids = create_measurement(targets, locations)
measurements = get_measurement(measurement_ids)
probes_written = write_probe_id(measurements, client)
write_measurement(probes_written, client)
write_result(probes_written, client)