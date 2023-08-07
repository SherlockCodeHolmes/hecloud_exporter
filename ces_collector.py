import asyncio
import aiohttp
import itertools
from aiohttp import TCPConnector
from cachetools import TTLCache
from prometheus_client.core import GaugeMetricFamily
from instances import merge_instances
from utils import merge_dict_lists, get_metrics, fetch_data

instance_list = merge_instances()


# 获取监控指标，单次查询只能返回1000个结果
async def get_all_metrics(session, namespace, params=None):
    """

    :rtype: list [{'namespace': 'SYS.ELB', 'dimensions': [{'name': 'lbaas_instance_id', 'value':
    '0dd521b7-14c1-4b77-bd28-848a49b19b73'}], 'metric_name': 'l4_con_usage', 'unit': '%'}]
    """
    if params is None:
        params = {"namespace": namespace}
    all_data = []
    while True:
        data = await get_metrics(session, namespace, params)
        metrics = data.get("metrics", [])
        all_data.extend(metrics)
        # 如果返回的metrics少于1000，说明已经获取了所有数据
        if len(metrics) < 1000:
            break
        # 检查返回的marker值
        meta_data = data.get("meta_data")
        marker = meta_data.get("marker")
        # 更新start参数
        params["start"] = marker
    return all_data


def filter_metric(metrics):
    instance1 = [{"name": subitem["name"], "value": subitem["value"]} for item in instance_list for subitem in
                 item["dimensions"]]
    # 只保留指定实例指标
    metrics1 = []
    for metric in metrics:
        if any(dim in instance1 for dim in metric["dimensions"]):
            metrics1.append(metric)

    # 删除unit字段
    metrics2 = [{k: v for k, v in metric.items() if k != 'unit'} for metric in metrics1]
    return metrics2


# 定义一个缓存
cache = TTLCache(maxsize=20, ttl=86400)


async def get_all_metrics_for_namespaces(session, namespaces):
    if 'metrics' not in cache:
        tasks = [get_all_metrics(session, ns) for ns in namespaces]
        metric_results = await asyncio.gather(*tasks)
        metrics = list(itertools.chain.from_iterable(metric_results))
        metric_filter = filter_metric(metrics)
        cache['metrics'] = metric_filter

    return cache['metrics']


# 异步获取监控数据，单次只能查询500组数据
async def batch_query(batch_size: int = 500):
    async with aiohttp.ClientSession(connector=TCPConnector(ssl=False)) as session:
        namespaces = ["SYS.ELB", "SYS.RDS", "SYS.DCS", "SYS.DMS"]
        metric_filter = await get_all_metrics_for_namespaces(session, namespaces)
        metric_data = []
        for i in range(0, len(metric_filter), batch_size):
            batch = metric_filter[i:i + batch_size]
            query_result = await fetch_data(session, batch)
            metric_data.extend(query_result['metrics'])

        return metric_data


# 添加实例名称指标
def add_name_label():
    metric_data = asyncio.run(batch_query())
    for point in metric_data:
        for instance in instance_list:
            if any(i in point['dimensions'] for i in instance['dimensions']):
                point['dimensions'] = merge_dict_lists(point['dimensions'], instance['dimensions'])
                break
    return metric_data


class CesCollector:
    def collect(self):
        metric_data = add_name_label()
        gauges = {}
        for entry in metric_data:
            metric_name = entry['namespace'].split(".")[1].lower() + '_' + entry['metric_name'].replace('.', '_')
            dimensions = {d['name']: d['value'] for d in entry['dimensions']}
            datapoints = entry['datapoints']
            if metric_name not in gauges:
                gauges[metric_name] = GaugeMetricFamily(metric_name, '', labels=dimensions.keys())
            if datapoints:
                first_datapoint = datapoints[0]['average']
                gauges[metric_name].add_metric(dimensions.values(), first_datapoint)

        for gauge in gauges.values():
            yield gauge
