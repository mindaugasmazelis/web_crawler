from proxy import proxies
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import sys
import concurrent.futures
import time
import traceback


def choose_URL(mined_df):
    inner_lead_df = mined_df.loc[(mined_df['lead_visited'] == False)&(mined_df['working_now']==False), :]
    # print("Whole mined df size: {}, ".format(len(inner_lead_df),))
    if len(inner_lead_df)<1:
        print(r"All URLs used. Waiting...")
        time.sleep(3)
    record_line = inner_lead_df.loc[inner_lead_df['recorded_time'] == inner_lead_df['recorded_time'].min(), :].sample(n=1)

    lead = record_line.iloc[0]["lead"]
    if "https://" not in lead:
        lead = "https://www.15min.lt"+lead
    return lead

def select_proxy(proxy_df):
    inner_lead_df = proxy_df.loc[proxy_df['working'] == True, :]
    # rng = random.randint(0, len(inner_lead_df))
    # print(rng)
    if len(inner_lead_df)<1:
        print(r"All proxies used.")
        reset_proxy()
    if len(inner_lead_df.loc[inner_lead_df['last_used'] != 0, :]) > 1:
        record_line = inner_lead_df.loc[inner_lead_df['last_used'] == inner_lead_df['last_used'].min(), :].sample(n=1)
        if (time.time() - inner_lead_df['last_used'].min()) < 3:
            time.sleep(3)
    else:
        record_line = inner_lead_df.sample(n=1)
        # record_line["last_used"] = time.time()
    return record_line

def reset_proxy():
    p = proxies()
    proxy_array = p.get_proxies()
    with open('ip_dict.json') as f:
        all_ip_dict = json.load(f)
    proxy_df = pd.DataFrame(all_ip_dict).transpose().astype(dtype=PROXY_DTYPES)

def get_teh_fuckin_result(URL, PROXIES, mined_data):
    try:
        # temp_df = {}
        mined_data.loc[mined_data['lead'] == URL, 'working_now'] = True
        r = requests.get(URL, proxies=PROXIES, headers=headers, timeout=30).content
        soup = BeautifulSoup(r, "html.parser")
        links = list(set([link["href"] for link in soup.findAll("a", href=True)]))
        temp_df = pd.DataFrame({
            "visited_website": URL,
            "lead": links,
            "recorded_time": time.time(),
            "lead_visited": False,
            "working_now": False
        })
        mined_data = mined_data.append(temp_df, ignore_index=True)
        proxy_df.loc[random_proxy.index[0], ('last_used')] = time.time()
        mined_data.to_csv('mined_data.csv', mode='a', header=False)
        print("{} lines uploaded".format(len(mined_data)))
        mined_data.loc[mined_data['lead'] == URL, 'working_now'] = False
        return mined_data
    except Exception as e:
        proxy_df.loc[random_proxy.index[0], ('working')] = False
        # print(traceback.format_exc())
        # print("Error: {}".format(e))

URL = r'https://www.15min.lt/'
PROXY_DTYPES = {
    'ip': 'object',
    'port': 'object',
    'working': 'bool',
    'last_used': 'float64'
}

MINED_DTYPES = {}
MINED_STRUCTURE = ["visited_website","lead","recorded_time","lead_visited"]
mined_data = pd.DataFrame(columns=MINED_STRUCTURE)
mined_data['working_now'] = False # adds field for multithreading
# print(len(mined_data))


print("start")
p = proxies()

proxy_array = p.get_proxies()
with open('ip_dict.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(proxy_array))

with open('ip_dict.json') as f:
    all_ip_dict = json.load(f)
proxy_df = pd.DataFrame(all_ip_dict).transpose().astype(dtype=PROXY_DTYPES)
print("proxy list obtained")

a = True
while a:
    try:
        # print("scanning URL: {}".format(URL))
        random_proxy = select_proxy(proxy_df)
        headers = p.USER_AGENT
        ip_port = ('{}:{}').format(random_proxy.iloc[0]['ip'], random_proxy.iloc[0]['port'])
        PROXIES = {'https': ip_port, 'http': ip_port}
        returned_data = get_teh_fuckin_result(URL, PROXIES, mined_data)
        if len(returned_data)>1:
            a = False
    except Exception as e:
        pass

with concurrent.futures.ThreadPoolExecutor() as exec:
    while True:
        try:
            URL = choose_URL(returned_data)
            random_proxy = select_proxy(proxy_df)
            headers = p.USER_AGENT
            ip_port = ('{}:{}').format(random_proxy.iloc[0]['ip'], random_proxy.iloc[0]['port'])
            PROXIES = {'https': ip_port, 'http': ip_port}

            # print("scanning URL: {}".format(URL))
            res = exec.submit(get_teh_fuckin_result, URL, PROXIES, mined_data)

        except Exception as e:
            mined_data.loc[mined_data['lead'] == URL, 'working_now'] = False
            proxy_df.loc[random_proxy.index[0],('working')] = False
            # print(traceback.format_exc())
            # print("Error: {}".format(e))