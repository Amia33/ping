"""Main script to measure site speed"""

from modules import (
    get_vars,
    get_probes,
    create_measurement,
    get_results,
    insert_measurements,
    insert_results
)
from pymongo import MongoClient

env = get_vars()
client = MongoClient(env["mongodb_str"])
probes = get_probes(client)
measurements = create_measurement(env)
results = get_results(probes, measurements)
if results:
    insert_measurements(client, results)
    insert_results(client, results)
client.close()
