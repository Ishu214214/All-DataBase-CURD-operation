import psycopg2
from opensearchpy import OpenSearch, RequestsHttpConnection
import json
import re
import os
import threading


redshift_host = "ubuy-countramazonaws.com"
redshift_port = "5439"  # Default Redshift port
redshift_dbname = ""
redshift_user = ""
redshift_password = ""

domain_list = {"saudi": "www.com.sa"}
index_name = "nginx-access-*"
thread_list = []


def new_connection():
    host = "search-ubuylzonaws.com"
    port = 443
    username = ""
    password = "@"
    client = OpenSearch(
        hosts=[f"{host}:{port}"],
        http_auth=(username, password),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )
    return client


def fetchdata(country_name_local, domain_name_local):
    file_name = country_name_local.replace(" ", "_")
    file_name = file_name.lower()
    json_file_path = (
        f"/home/{file_name}_data.json"
    )
    last_date_path = (
        "/home//test/log/last_date_"
        + str(file_name)
        + ".json"
    )
    if not os.path.exists(last_date_path):
        last_date = "now-3d/d"
    else:
        with open(last_date_path, "r") as json_file:
            last_date = json_file.read().strip()

    if last_date == "":
        last_date = "now-3d/d"

    print(str(last_date_path) + "===last_date===" + str(last_date))

    processed_dates = []
    while 1:
        query = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"request": domain_name_local}},
                        {"range": {"@timestamp": {"gte": last_date}}},
                    ],
                    "should": [],
                    "must_not": [],
                }
            },
            "_source": [
                "@timestamp",
                "@date",
                "cf_country",
                "IP",
                "request",
                "user_agent",
                "referrer",
                "response",
                "response_time",
            ],
            "size": 10000,
            "sort": [{"@timestamp": {"order": "asc", "unmapped_type": "boolean"}}],
        }
        print(query)

        connection2 = new_connection()
        data_count = 0
        try:
            result = connection2.search(index=index_name, body=query)

            hits = result["hits"]["hits"]
            data_count = len(hits)
            data = []
            if data_count:
                for hit in hits:
                    source = hit["_source"]
                    last_date = source["@timestamp"]
                    source.pop("@timestamp")
                    source["date"] = last_date
                    # print(last_date)

                    if "request" in source and source["request"]:
                        regex = "\.([a-zA-Z0-9]+)(?:[?#]|$)"
                        request = source["request"]

                        req_match = re.findall(regex, request)
                        if len(req_match):
                            filetype = req_match[0]
                        else:
                            filetype = ""

                    if "referrer" in source and source["referrer"]:
                        regex = "http[s]?://[^/]+/([^/]+)"
                        referrer = source["referrer"]
                        ref_match = re.findall(regex, referrer)
                        if len(ref_match):
                            first_path = ref_match[0]
                        else:
                            first_path = ""

                    if "user_agent" in source and source["user_agent"]:
                        regex = "(?i)(adsbot\-google|feedfetcher\-google|storebot\-google|googleother|googlebot\-image|googlebot\-video|googlebot|bingbot|gptbot|yandex|baiduspider|facebot|facebookexternalhit|twitterbot|rogerbot|linkedinbot|embedly|quora link preview|showyoubot|outbrain|pinterest|slackbot|vkShare|w3c_validator)"
                        user_agent = source["user_agent"]
                        agent_match = re.findall(regex, user_agent)
                        if len(agent_match):
                            user_agent_type = agent_match[0]
                        else:
                            user_agent_type = ""

                    if "date" in source and source["date"]:
                        date_data = source["date"]
                        if "T" in date_data:
                            date_data = date_data.split("T")
                            Date = date_data[0]
                            Time_data = date_data[1]
                            if "." in Time_data:
                                Time = Time_data.split(".")[0]
                    source.pop("date")
                    source.update(
                        {
                            "filetype": filetype,
                            "first_path": first_path,
                            "user_agent_type": user_agent_type,
                            "Date": Date,
                            "Time": Time,
                        }
                    )
                    data.append(source)
            else:
                break
            print(
                "file write==="
                + str(last_date_path)
                + "==="
                + str(last_date)
                + "===="
                + str(data_count)
            )
            with open(last_date_path, "w") as f:
                f.write(str(last_date))

            with open(json_file_path, "w") as json_file:
                json.dump(data, json_file)

            upload_connect_redshift(file_name, country_name_local)
        except Exception as e:
            print(f"An error occurred: {e}")

        if data_count < 100:
            print("true condition")
            break


def upload_connect_redshift(file_name, country_name_local):
    print("uploading data into bq+++++++++++++++++++++++++++")
    conn = psycopg2.connect(
        dbname=redshift_dbname,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port,
    )
    cur = conn.cursor()
    json_file_path = (
        f"/home/log/{file_name}_data.json"
    )
    with open(json_file_path) as f:
        data = json.load(f)

    for record in data:
        cur.execute(
            f"""INSERT INTO {country_name_local}.{country_name_local} (cf_country, IP, request, user_agent, referrer, first_path, filetype, user_agent_type, response, response_time, Date, Time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """,
            (
                record.get("cf_country"),
                record.get("IP"),
                record.get("request"),
                record.get("user_agent"),
                record.get("referrer"),
                record.get("first_path"),
                record.get("filetype"),
                record.get("user_agent_type"),
                record.get("response"),
                record.get("response_time"),
                record.get("Date"),
                record.get("Time"),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


for country_name in domain_list:
    domain_name = domain_list[country_name]
    t1 = threading.Thread(
        target=fetchdata,
        args=(
            country_name,
            domain_name,
        ),
    )
    t1.start()
    thread_list.append(t1)
    t1.join()

# conn = upload_connect_redshift()
# cur = conn.cursor()
# cur.execute("SELECT datname FROM pg_database;")
# databases = cur.fetchall()
# for db in databases:
#     print(db[0])

# cur.close()
# conn.close()
