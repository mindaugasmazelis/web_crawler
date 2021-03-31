import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import sys
import concurrent.futures


class proxies:
    def __init__(self):
        self.PROXY_URL = r'https://free-proxy-list.net/'
        self.USER_AGENT = {'User-agent': 'Mozilla/5.0'}
        self.IP_CHECK_URL = r'https://api.myip.com'

    def get_proxies(self):
        ip_dict = self.get_all_ips()
        validated = self.validate_ips(ip_dict)
        return validated

    def get_all_ips(self):
        r = requests.get(url=self.PROXY_URL, headers=self.USER_AGENT).content
        soup = BeautifulSoup(r, 'html.parser')
        containers = soup.find('table', {'class': 'table table-striped table-bordered'}).findAll('tr')[1:]
        temp_dict = {}
        index = 0
        for cont in containers:
            sub_conts = cont.findAll('td')
            if len(sub_conts) > 0:
                index += 1
                ip = sub_conts[0].text
                port = sub_conts[1].text
                temp_dict[index] = {}
                temp_dict[index]['ip'] = ip
                temp_dict[index]['port'] = port
                temp_dict[index]['working'] = None
                temp_dict[index]['last_used'] = 0.0
        return temp_dict

    def individual_thread_ip_check(self, key, PROXIES):
        try:
            r = requests.get(self.IP_CHECK_URL, proxies=PROXIES, headers=self.USER_AGENT, timeout=20).content
            RETURNED_IP = json.loads(r)['ip']
            if RETURNED_IP == self.all_ip_dict[key]['ip']:
                self.all_ip_dict[key]['working'] = True
            else:
                self.all_ip_dict[key]['working'] = False
        except Exception as e:
            self.all_ip_dict[key]['working'] = False
        return None

    def validate_ips(self, dict):
        self.dict = dict
        self.all_ip_dict = json.loads(json.dumps(self.dict))
        with concurrent.futures.ThreadPoolExecutor() as exec:
            for key in self.all_ip_dict.keys():
                ip_port = ('{}:{}').format(self.all_ip_dict[key]['ip'], self.all_ip_dict[key]['port'])
                PROXIES = {
                    'https': ip_port,
                    'http': ip_port
                }
                res = exec.submit(self.individual_thread_ip_check, key, PROXIES)
        return self.all_ip_dict


if __name__ == '__main__':
    p = proxies()
    validated = p.get_proxies()
    with open('ip_dict.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(validated))
    print('All done!')