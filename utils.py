import json
import re
import yaml
import time
import requests
from urllib.parse import urlencode
from apig_sdk import signer


# 合并两个包含字典的列表，并去重
def merge_dict_lists(list1, list2):
    merged_list = list1 + list2
    unique_dict_list = []
    for item in merged_list:
        if item not in unique_dict_list:
            unique_dict_list.append(item)
    return unique_dict_list


def contains_keywords(s):
    pattern = r'(stream|xinling)'
    return bool(re.search(pattern, s))


with open('ces_exporter.yml', 'r', encoding='utf-8') as config_file:
    cfg = yaml.load(config_file, Loader=yaml.FullLoader)


def heyingyun_api(namespace, project_id=cfg['project_id'],
                  endpoint=cfg['endpoint'], method='GET', path='instances', params=None, body=None, api_version='v1.0'):
    if body is None:
        body = {}
    if params is None:
        params = {}
    sig = signer.Signer()
    sig.Key = cfg['access_key']
    sig.Secret = cfg['access_secret']
    headers = {"content-type": "application/json"}
    host = f'{namespace}.{endpoint}'
    url = f'https://{host}/{api_version}/{project_id}/{path}'
    r = signer.HttpRequest(method, url + '?' + urlencode(params))
    r.body = json.dumps(body)
    r.headers = headers
    sig.Sign(r)
    resp = requests.request(r.method, r.scheme + "://" + r.host + r.uri, headers=r.headers, data=r.body)
    return json.loads(resp.content)


async def fetch_data(session, batch, payload=None, project_id=cfg['project_id'], endpoint=cfg['endpoint']):
    if payload is None:
        payload = {
            "metrics": batch,
            "from": int(time.time() * 1000) - 300000,
            "to": int(time.time() * 1000),
            "period": "300",
            "filter": "average"
        }
    sig = signer.Signer()
    sig.Key = cfg['access_key']
    sig.Secret = cfg['access_secret']
    headers = {"content-type": "application/json"}
    url = f'https://ces.{endpoint}/V1.0/{project_id}/batch-query-metric-data'
    r = signer.HttpRequest('POST', url)
    r.body = json.dumps(payload)
    r.headers = headers
    sig.Sign(r)
    async with session.post(url, headers=r.headers, data=r.body) as response:
        if response.status == 200:
            data = await response.json()
            return data
        else:
            return {"metrics": ""}


async def get_metrics(session, namespace, params=None, project_id=cfg['project_id'], endpoint=cfg['endpoint']):
    sig = signer.Signer()
    sig.Key = cfg['access_key']
    sig.Secret = cfg['access_secret']
    headers = {"content-type": "application/json"}
    host = f"ces.{endpoint}"
    url = f"https://{host}/V1.0/{project_id}/metrics"
    r = signer.HttpRequest('GET', url + '?' + urlencode(params))
    r.headers = headers
    sig.Sign(r)
    async with session.get(url, headers=r.headers, params=params) as response:
        if response.status == 200:
            return await response.json()
        else:
            return {"metrics": ""}
