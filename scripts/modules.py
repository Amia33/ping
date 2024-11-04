"""Modules for index.py"""

from os import (
    getenv,
    path
)
from time import sleep
from datetime import (
    datetime)
from dotenv import load_dotenv
from pandas import DataFrame
import requests


def get_vars():
    """Get variables from system"""
    env = {
        "targets": [],
        "mongodb_str": "",
        "globalping_apikey": ""
    }
    if path.isfile("scripts/.env"):
        load_dotenv("scripts/.env")
    env["targets"] = getenv("TARGETS").split(",")
    env["mongodb_str"] = getenv("MONGODB_STR")
    env["globalping_apikey"] = getenv("GLOBALPING_APIKEY")
    return env


def get_probes(client):
    """Get probes list from mongodb"""
    probes_db = client["ping"]["probes"]
    dest = "https://api.globalping.io/v1/probes"
    head = {
        "User-Agent": "ping.amia.work (https://github.com/Amia33/ping)",
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    resp = requests.get(url=dest, headers=head, timeout=15)
    probes_get = resp.json()
    newprobe_id = probes_db.count_documents({}) + 1
    newprobes = resolve_newprobes(probes_db, probes_get, newprobe_id)
    if newprobes:
        probes_db.insert_many(newprobes)
    probes = DataFrame(probes_db.find()).sort_values("_id")
    return probes


def resolve_newprobes(probes_db, probes_get, newprobe_id):
    """Resolve probes to insert"""
    newprobes = []
    skip_verify = newprobe_id == 1
    if not skip_verify:
        probes = DataFrame(probes_db.find()).sort_values("_id")
    for probe in probes_get:
        data = probe["location"]
        if not skip_verify:
            probes_search = probes.loc[
                (probes["country"] == data["country"]) &
                (probes["city"] == data["city"]) &
                (probes["asn"] == data["asn"]) &
                (probes["network"] == data["network"]) &
                (probes["latitude"] == data["latitude"]) &
                (probes["longitude"] == data["longitude"])
            ]
            if not probes_search.empty:
                continue
        data["_id"] = newprobe_id
        newprobes.append(data)
        newprobe_id += 1
    return newprobes


def create_measurement(env):
    """Create measurements"""
    targets = env["targets"]
    globalping_apikey = env["globalping_apikey"]
    measurements = []
    dest = "https://api.globalping.io/v1/measurements"
    head = {
        "User-Agent": "ping.amia.work (https://github.com/Amia33/ping)",
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + globalping_apikey
    }
    data = {
        "type": "mtr",
        "target": "",
        "locations": [
            {
            "magic": "world",
            "limit": 10
            }
        ],
        "measurementOptions": {
            "port": 443,
            "protocol": "tcp",
            "ipVersion": 4,
            "packets": 16
        }
    }
    for target in targets:
        data["target"] = target
        resp = requests.post(url=dest, headers=head, json=data, timeout=15)
        measurement = resp.json()["id"]
        measurements.append(measurement)
    return measurements


def get_results(probes, measurements):
    """Get results of measurements"""
    results = []
    head = {
        "User-Agent": "ping.amia.work (https://github.com/Amia33/ping)",
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    for measurement in measurements:
        dest = "https://api.globalping.io/v1/measurements/" + measurement
        resp = requests.get(url=dest, headers=head, timeout=15)
        result = resp.json()
        while result["status"] == "in-progress":
            sleep(1)
            resp = requests.get(url=dest, headers=head, timeout=15)
            result = resp.json()
        result = filter_result(probes, result)
        if result["results"]:
            results.append(result)
    return results


def filter_result(probes, result):
    """Filter out failed results"""
    filtered_sub_results = []
    for sub_result in result["results"]:
        if sub_result["result"]["status"] != "finished":
            continue
        for hop in sub_result["result"]["hops"]:
            if not hop["timings"]:
                break
        else:
            probes_search = probes.loc[
                (probes["country"] == sub_result["probe"]["country"]) &
                (probes["city"] == sub_result["probe"]["city"]) &
                (probes["asn"] == sub_result["probe"]["asn"]) &
                (probes["network"] == sub_result["probe"]["network"]) &
                (probes["latitude"] == sub_result["probe"]["latitude"]) &
                (probes["longitude"] == sub_result["probe"]["longitude"])
            ]
            sub_result["probe"] = probes_search["_id"].tolist()[0]
            filtered_sub_results.append(sub_result)
    result["results"] = filtered_sub_results
    return result


def insert_measurements(client, results):
    """Insert data for measurements"""
    measurements_db = client["ping"]["measurements"]
    new_measurements = []
    for result in results:
        created = datetime.strptime(
            result["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        updated = datetime.strptime(
            result["updatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duration = (updated - created).total_seconds()
        probes = []
        for sub_result in result["results"]:
            probes.append(sub_result["probe"])
        data = {
            "_id": result["id"],
            "target": result["target"],
            "createdAt": created,
            "duration": duration,
            "probes": probes
        }
        new_measurements.append(data)
    measurements_db.insert_many(new_measurements)


def insert_results(client, results):
    """Insert data for results"""
    results_db = client["ping"]["results"]
    new_results = []
    for result in results:
        for sub_result in result["results"]:
            hops = []
            for hop in sub_result["result"]["hops"]:
                hop_data = {}
                hop_data["stats"] = hop["stats"]
                try:
                    hop_data["asn"] = hop["asn"][0]
                except IndexError:
                    hop_data["asn"] = 0
                hop_timing = []
                for timing in hop["timings"]:
                    hop_timing.append(timing["rtt"])
                hop_data["timings"] = hop_timing
                hop_data["resolvedAddress"] = hop["resolvedAddress"]
                hops.append(hop_data)
            data = {
                "_id": result["id"] + "-" + str(sub_result["probe"]),
                "resolvedAddress": sub_result["result"]["resolvedAddress"],
                "hops": hops
            }
            new_results.append(data)
    results_db.insert_many(new_results)
