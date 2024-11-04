# pylint: disable=locally-disabled, unsubscriptable-object
"""Modules"""

from time import sleep
from datetime import (
    datetime,
    timezone,
    timedelta
)
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
from pandas import (
    DataFrame,
    json_normalize
)
import requests


def sync_probe(client):
    """Sync probes currently online"""
    dest = "https://api.globalping.io/v1/probes"
    head = {
        "User-Agent": "ping.amia.work (https://github.com/Amia33/ping)",
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    resp = requests.get(url=dest, headers=head, timeout=15)
    probes = resp.json()
    


def create_measurement(targets, locations):
    """Create a measurement"""
    measurement_ids = []
    dest = "https://api.globalping.io/v1/measurements"
    head = {
        "User-Agent": "ping.amia.work (https://github.com/Amia33/Globalping)",
        "Accept-Encoding": "gzip",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    data = {
        "type": "ping",
        "target": "",
        "locations": "",
        "measurementOptions": {
            "packets": 16
        }
    }
    for target in targets:
        if measurement_ids:
            locations = measurement_ids[0]
        data["target"] = target
        data["locations"] = locations
        resp = requests.post(url=dest, headers=head, json=data, timeout=15)
        measurement_id = resp.json()["id"]
        measurement_ids.append(measurement_id)
    return measurement_ids


def get_measurement(measurement_ids):
    """Get a measurement by ID"""
    measurements = []
    head = {
        "User-Agent": "ping.amia.work (https://github.com/Amia33/Globalping)",
        "Accept-Encoding": "gzip",
        "Accept": "application/json"
    }
    for measurement_id in measurement_ids:
        dest = "https://api.globalping.io/v1/measurements/" + measurement_id
        resp = requests.get(url=dest, headers=head, timeout=15)
        measurement = resp.json()
        while measurement["status"] == "in-progress":
            sleep(0.5)
            resp = requests.get(url=dest, headers=head, timeout=15)
            measurement = resp.json()
        filtered_results = []
        for result in measurement["results"]:
            if result["result"]["status"] != "finished":
                continue
            if not result["result"]["timings"]:
                continue
            filtered_results.append(result)
        measurement["results"] = filtered_results
        measurements.append(measurement)
    return measurements


def connect(conn_str):
    """Connect to MongoDB Atlas"""
    client = MongoClient(conn_str)["ping"]
    return client


def write_probe_id(measurements, client):
    "Write probe id"
    for measurement in measurements:
        current_probes = DataFrame(client["probes"].find()).sort_values("_id")
        upload = []
        try:
            new_probe = current_probes["_id"].tolist()[-1] + 1
        except KeyError:
            new_probe = 1
        for result in measurement["results"]:
            try:
                search_probes = current_probes.loc[
                    (current_probes["region"] == result["probe"]["region"]) &
                    (current_probes["country"] == result["probe"]["country"]) &
                    (current_probes["city"] == result["probe"]["city"]) &
                    (current_probes["asn"] == result["probe"]["asn"]) &
                    (current_probes["network"] == result["probe"]["network"]) &
                    (current_probes["latitude"] == result["probe"]["latitude"]) &
                    (current_probes["longitude"] ==
                     result["probe"]["longitude"])
                ]
            except KeyError:
                create_probe = True
            else:
                create_probe = search_probes.empty
            finally:
                if create_probe is True:
                    probe_id = new_probe
                    probe_data = {
                        "_id": probe_id,
                        "region": result["probe"]["region"],
                        "country": result["probe"]["country"],
                        "city": result["probe"]["city"],
                        "asn": result["probe"]["asn"],
                        "network": result["probe"]["network"],
                        "latitude": result["probe"]["latitude"],
                        "longitude": result["probe"]["longitude"],
                    }
                    upload.append(probe_data)
                    new_probe += 1
                else:
                    probe_id = search_probes["_id"].tolist()[0]
            result["probe"] = probe_id
        if upload:
            client["probes"].insert_many(upload)
    return measurements


def write_measurement(probes_written, client):
    """Write measurement"""
    upload = []
    colle = "measurements-" + datetime.now(timezone.utc).strftime("%Y%m%d")
    for measurement in probes_written:
        created_time = datetime.strptime(
            measurement["createdAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        updated_time = datetime.strptime(
            measurement["updatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duration = (updated_time - created_time).total_seconds()
        probe_ids = []
        for result in measurement["results"]:
            probe_ids.append(result["probe"])
        measurement_data = {
            "_id": measurement["id"],
            "target": measurement["target"],
            "duration": duration,
            "probe_ids": probe_ids
        }
        upload.append(measurement_data)
    client[colle].insert_many(upload)


def write_result(probes_written, client):
    """Write result"""
    upload = []
    colle = "results-" + datetime.now(timezone.utc).strftime("%Y%m%d")
    for measurement in probes_written:
        for result in measurement["results"]:
            rtts = []
            for packet in result["result"]["timings"]:
                rtts.append(packet["rtt"])
            result_data = {
                "_id": measurement["id"] + "-" + str(result["probe"]),
                "timings": {
                    "min": result["result"]["stats"]["min"],
                    "max": result["result"]["stats"]["max"],
                    "total": round(sum(rtts), 3)
                },
                "packets": {
                    "total": result["result"]["stats"]["total"],
                    "rcv": result["result"]["stats"]["rcv"]
                }
            }
            upload.append(result_data)
    client[colle].insert_many(upload)


def daily_measurement(target, yesterday, filtered_measurements):
    """Calculate daily measurement"""
    durations = filtered_measurements["duration"].tolist()
    probe_ids = filtered_measurements["probe_ids"].tolist()
    probe_counts = [len(probe_id) for probe_id in probe_ids]
    data = {
        "_id": target + "-" + yesterday,
        "expires": datetime.now(timezone.utc) + timedelta(days=370),
        "count": len(filtered_measurements),
        "duration": {
            "min": min(durations),
            "max": max(durations),
            "total": round(sum(durations), 3)
        },
        "probe_ids": {
            "min": min(probe_counts),
            "max": max(probe_counts),
            "total": sum(probe_counts)
        }
    }
    return data


def filter_result(filtered_measurements, probes_count):
    """Filter result and group them by probe id"""
    data = {probe_id: [] for probe_id in range(1, probes_count+1)}
    for measurement in filtered_measurements.itertuples(index=False):
        for probe_id in measurement[-1]:
            data[probe_id].append(measurement[0] + "-" + str(probe_id))
    for probe_id in range(1, probes_count+1):
        if not data[probe_id]:
            data.pop(probe_id, None)
    return data


def write_daily_results(target, probes_count, results_df, filtered_result_ids):
    """Calculate daily results"""
    results_upload = []
    prefix = target + "-" + (datetime.now(timezone.utc) -
                             timedelta(days=1)).strftime("%Y%m%d") + "-"
    for probe_id in range(1, probes_count + 1):
        try:
            filtered_results = results_df.loc[
                (results_df["_id"].isin(filtered_result_ids[probe_id]))
            ]
        except KeyError:
            continue
        timings = json_normalize(filtered_results["timings"])
        packets = json_normalize(filtered_results["packets"])
        data = {
            "_id": prefix + str(probe_id),
            "expires": datetime.now(timezone.utc) + timedelta(days=370),
            "count": len(filtered_results),
            "timings": {
                "min": min(timings["min"].tolist()),
                "max": max(timings["max"].tolist()),
                "total": round(sum(timings["total"].tolist()), 3)
            },
            "packets": {
                "total": {
                    "min": min(packets["total"].tolist()),
                    "max": max(packets["total"].tolist()),
                    "total": sum(packets["total"].tolist())
                },
                "rcv": {
                    "min": min(packets["rcv"].tolist()),
                    "max": max(packets["rcv"].tolist()),
                    "total": sum(packets["rcv"].tolist())
                }
            }
        }
        results_upload.append(data)
    return results_upload


def write_daily(targets, client):
    """Write daily"""
    yesterday = (datetime.now(timezone.utc) -
                 timedelta(days=1)).strftime("%Y%m%d")
    measurements_df = DataFrame(client["measurements-" + yesterday].find())
    results_df = DataFrame(client["results-" + yesterday].find())
    probes_count = DataFrame(client["probes"].find()).sort_values("_id")[
        "_id"].tolist()[-1]
    measurements_upload = []
    for target in targets:
        filtered_measurements = measurements_df.loc[
            (measurements_df["target"] == target)
        ]
        filtered_result_ids = filter_result(
            filtered_measurements, probes_count)
        measurements_data = daily_measurement(
            target, yesterday, filtered_measurements)
        results_upload = write_daily_results(
            target, probes_count, results_df, filtered_result_ids)
        client["results"].insert_many(results_upload)
        measurements_upload.append(measurements_data)
    client["measurements"].insert_many(measurements_upload)


def drop_colle(client):
    """Drop temporary collections"""
    yesterday = (datetime.now(timezone.utc) -
                 timedelta(days=1)).strftime("%Y%m%d")
    client["measurements-" + yesterday].drop()
    client["results-" + yesterday].drop()


def output_probes(client):
    """Output probes.md"""
    probes_df = DataFrame(client["probes"].find()).sort_values("_id")
    with open("results/source/_posts/probes.md", "w", encoding="utf-8") as f:
        f.write("---\n" +
                "title: Probes\n" +
                "date: 2024/08/01 00:00:00\n" +
                "updated: " +
                datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S") +
                "\ncomments: false\n" +
                "categories: Probes\n" +
                "sticky: 50\n" +
                "---\n\n" +
                "**" +
                str(len(probes_df)) +
                "** probes have been registered.\n\n" +
                "<!-- more -->\n\n" +
                "|Probe ID|Region|Country|City|ASN|Network|Latitude|Longitude|\n" +
                "|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|\n")
        for probe in probes_df.itertuples(index=False):
            f.write("|" + str(probe[0]) + "|" +
                    probe[1] + "|" +
                    probe[2] + "|" +
                    probe[3] + "|" +
                    str(probe[4]) + "|" +
                    probe[5] + "|" +
                    str(probe[6]) + "|" +
                    str(probe[7]) + "|\n")


def measurement_table(targets, measurements_df, yesterday, f):
    """Iterate write objects for measurement"""
    for i, target in enumerate(targets):
        search_measurements = measurements_df.loc[
            (measurements_df["_id"] == target +
             "-" + yesterday.strftime("%Y%m%d"))
        ]
        for row in search_measurements.itertuples(index=False):
            duration_avg = round((row[3]["total"] / row[2]), 3)
            probes_avg = round((row[4]["total"] / row[2]), None)
            f.write("|" + str(i) + "|" +
                    str(row[2]) + "|" +
                    str(row[3]["min"]) + "|" +
                    str(row[3]["max"]) + "|" +
                    str(duration_avg) + "|" +
                    str(row[4]["min"]) + "|" +
                    str(row[4]["max"]) + "|" +
                    str(probes_avg) + "|\n")


def results_table(targets, results_df, yesterday, f):
    """Iterate write objects for result"""
    for i, target in enumerate(targets):
        search_results = results_df.loc[
            (results_df["_id"].str.startswith(target +
             "-" + yesterday.strftime("%Y%m%d"), na=False))
        ]
        f.write("\n## Probe-specific Data for Target " +
                str(i) + "\n\n" +
                "|Probe ID|Count|Timings[min]|Timings[max]|Timings[avg]|" +
                "Packets[min][total, rcv]|Packets[max][total, rcv]|Packets[avg][total, rcv]|" +
                "Percentage of Loss[avg]|\n" +
                "|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|\n")
        for row in search_results.itertuples(index=False):
            timings_avg = round((row[3]["total"] / row[4]["rcv"]["total"]), 3)
            packets_total_avg = round(
                (row[4]["total"]["total"] / row[2]), None)
            packets_rcv_avg = round((row[4]["rcv"]["total"] / row[2]), None)
            packets_drop_total = row[4]["total"]["total"] - \
                row[4]["rcv"]["total"]
            rate_avg = round((100 * packets_drop_total /
                             row[4]["total"]["total"]), 3)
            f.write("|" + row[0].split("-")[-1] + "|" +
                    str(row[2]) + "|" +
                    str(row[3]["min"]) + "|" +
                    str(row[3]["max"]) + "|" +
                    str(timings_avg) + "|" +
                    str(row[4]["total"]["min"]) + ", " + str(row[4]["rcv"]["min"]) + "|" +
                    str(row[4]["total"]["max"]) + ", " + str(row[4]["rcv"]["max"]) + "|" +
                    str(packets_total_avg) + ", " + str(packets_rcv_avg) + "|" +
                    str(rate_avg) + "|\n")


def output_daily_report(targets, client):
    """Output daily report.md"""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)
                 ).replace(hour=0, minute=0, second=0, microsecond=0)
    measurements_df = DataFrame(client["measurements"].find())
    results_df = DataFrame(client["results"].find())
    daily_name = "results/source/_posts/daily/" + \
        yesterday.strftime("%Y-%m-%d") + ".md"
    with open(daily_name, "w", encoding="utf-8") as f:
        f.write("---\n" +
                "title: 'Daily report of measurements: " +
                yesterday.strftime("%Y/%m/%d") + "'\n" +
                "date: " +
                yesterday.strftime("%Y/%m/%d %H:%M:%S") + "\n" +
                "updated: " +
                datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S") +
                "\ncomments: false\n" +
                "categories: Daily\n" +
                "---\n\n" +
                "## Measurements Data\n\n" +
                "|Target|Count|Duration[min]|Duration[max]|Duration[avg]|Probes[min]|" +
                "Probes[max]|Probes[avg]|\n" +
                "|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|\n")
        measurement_table(targets, measurements_df, yesterday, f)
        f.write("\n<!-- more -->\n")
        results_table(targets, results_df, yesterday, f)


def gendates_weekly():
    """Generate dates list of last week"""
    dates = []
    end = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(weeks=1)
    while start < end:
        dates.append(start)
        start += timedelta(days=1)
    return dates


def long_measurement_table(targets, measurements_df, dates, f):
    """Iterate write objects for measurement"""
    for i, target in enumerate(targets):
        search_ids = [target + "-" + date for date in dates]
        search_measurements = measurements_df.loc[
            (measurements_df["_id"].isin(search_ids))
        ]
        counts = sum(search_measurements["count"].tolist())
        durations = json_normalize(search_measurements["duration"])
        probes = json_normalize(search_measurements["probe_ids"])
        duration_avg = round((sum(durations["total"].tolist()) / counts), 3)
        probes_avg = round((sum(probes["total"].tolist()) / counts), None)
        f.write("|" + str(i) + "|" +
                str(counts) + "|" +
                str(min(durations["min"].tolist())) + "|" +
                str(max(durations["max"].tolist())) + "|" +
                str(duration_avg) + "|" +
                str(min(probes["min"].tolist())) + "|" +
                str(max(probes["max"].tolist())) + "|" +
                str(probes_avg) + "|\n")


def long_results_table(targets, results_df, dates, f, probes_count):
    """Iterate write objects for result"""
    for i, target in enumerate(targets):
        f.write("\n## Probe-specific Data for Target " +
                str(i) + "\n\n" +
                "|Probe ID|Count|Timings[min]|Timings[max]|Timings[avg]|" +
                "Packets[min][total, rcv]|Packets[max][total, rcv]|Packets[avg][total, rcv]|" +
                "Percentage of Loss[avg]|\n" +
                "|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|\n")
        for probe_id in range(1, probes_count + 1):
            search_ids = [target + "-" + date +
                          "-" + str(probe_id) for date in dates]
            search_results = results_df.loc[
                (results_df["_id"].isin(search_ids))
            ]
            if search_results.empty is True:
                continue
            timings = json_normalize(search_results["timings"])
            packets = json_normalize(search_results["packets"])
            packets_drop_total = sum(packets["total.total"].tolist()) - \
                sum(packets["rcv.total"].tolist())
            rate_avg = round((100 * packets_drop_total /
                             sum(packets["total.total"].tolist())), 3)
            f.write("|" + str(probe_id) + "|" +
                    str(sum(search_results["count"].tolist())) + "|" +
                    str(min(timings["min"].tolist())) + "|" +
                    str(max(timings["max"].tolist())) + "|" +
                    str(round((sum(timings["total"].tolist()) /
                               sum(packets["rcv.total"].tolist())), 3)) + "|" +
                    str(min(packets["total.min"].tolist())) + ", " +
                    str(min(packets["rcv.min"].tolist())) + "|" +
                    str(max(packets["total.max"].tolist())) + ", " +
                    str(max(packets["rcv.max"].tolist())) + "|" +
                    str(round(
                        (sum(packets["total.total"].tolist()) /
                         sum(search_results["count"].tolist())),
                        None)) +
                    ", " + str(round(
                        (sum(packets["rcv.total"].tolist()) /
                         sum(search_results["count"].tolist())),
                        None)) +
                    "|" + str(rate_avg) + "|\n")


def output_weekly_report(targets, client, dates):
    """Output weekly report.md"""
    measurements_df = DataFrame(client["measurements"].find())
    results_df = DataFrame(client["results"].find())
    probes_count = DataFrame(client["probes"].find()).sort_values("_id")[
        "_id"].tolist()[-1]
    filename = "results/source/_posts/weekly/" + \
        dates[0].strftime("%G-%V") + ".md"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("---\n" +
                "title: 'Weekly report of measurements: " +
                dates[0].strftime("Year %G Week %V") + "'\n" +
                "date: " +
                dates[0].strftime("%Y/%m/%d %H:%M:%S") + "\n" +
                "updated: " +
                datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S") +
                "\ncomments: false\n" +
                "categories: Weekly\n" +
                "---\n\n" +
                dates[0].strftime("Year %G Week %V") + " is a period from " +
                dates[0].strftime("%Y/%m/%d") + " to " + dates[-1].strftime("%Y/%m/%d") +
                ".\n\n## Measurements Data\n\n" +
                "|Target|Count|Duration[min]|Duration[max]|Duration[avg]|Probes[min]|" +
                "Probes[max]|Probes[avg]|\n" +
                "|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|\n")
        dates = [date.strftime("%Y%m%d") for date in dates]
        long_measurement_table(targets, measurements_df, dates, f)
        f.write("\n<!-- more -->\n")
        long_results_table(targets, results_df, dates, f, probes_count)


def gendates_monthly():
    """Generate dates list of last month"""
    dates = []
    end = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    start = end - relativedelta(months=1)
    while start < end:
        dates.append(start)
        start += timedelta(days=1)
    return dates


def output_monthly_report(targets, client, dates):
    """Output monthly report.md"""
    measurements_df = DataFrame(client["measurements"].find())
    results_df = DataFrame(client["results"].find())
    probes_count = DataFrame(client["probes"].find()).sort_values("_id")[
        "_id"].tolist()[-1]
    filename = "results/source/_posts/monthly/" + \
        dates[0].strftime("%Y-%m") + ".md"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("---\n" +
                "title: 'Monthly report of measurements: " +
                dates[0].strftime("%Y/%m") + "'\n" +
                "date: " +
                dates[0].strftime("%Y/%m/%d %H:%M:%S") + "\n" +
                "updated: " +
                datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S") +
                "\ncomments: false\n" +
                "categories: Monthly\n" +
                "---\n\n" +
                "## Measurements Data\n\n" +
                "|Target|Count|Duration[min]|Duration[max]|Duration[avg]|Probes[min]|" +
                "Probes[max]|Probes[avg]|\n" +
                "|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|\n")
        dates = [date.strftime("%Y%m%d") for date in dates]
        long_measurement_table(targets, measurements_df, dates, f)
        f.write("\n<!-- more -->\n")
        long_results_table(targets, results_df, dates, f, probes_count)


def gendates_yearly():
    """Generate dates list of last year"""
    dates = []
    end = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    start = end - relativedelta(years=1)
    while start < end:
        dates.append(start)
        start += timedelta(days=1)
    return dates


def output_yearly_report(targets, client, dates):
    """Output yearly report.md"""
    measurements_df = DataFrame(client["measurements"].find())
    results_df = DataFrame(client["results"].find())
    probes_count = DataFrame(client["probes"].find()).sort_values("_id")[
        "_id"].tolist()[-1]
    filename = "results/source/_posts/yearly/" + \
        dates[0].strftime("%Y") + ".md"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("---\n" +
                "title: 'Monthly report of measurements: " +
                dates[0].strftime("Year %Y") + "'\n" +
                "date: " +
                dates[0].strftime("%Y/%m/%d %H:%M:%S") + "\n" +
                "updated: " +
                datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S") +
                "\ncomments: false\n" +
                "categories: Yearly\n" +
                "---\n\n" +
                "## Measurements Data\n\n" +
                "|Target|Count|Duration[min]|Duration[max]|Duration[avg]|Probes[min]|" +
                "Probes[max]|Probes[avg]|\n" +
                "|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|\n")
        dates = [date.strftime("%Y%m%d") for date in dates]
        long_measurement_table(targets, measurements_df, dates, f)
        f.write("\n<!-- more -->\n")
        long_results_table(targets, results_df, dates, f, probes_count)