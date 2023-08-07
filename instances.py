from utils import contains_keywords, heyingyun_api


def get_kafka_instances():
    kafka_list = heyingyun_api(
        namespace='dms',
        params={"engine": "kafka", "name": "hy-wx-produce"},
        api_version='v2'
    )
    data = [{"dimensions": [{"name": "kafka_instance_name", "value": i['name']},
                            {"name": "kafka_instance_id", "value": i['instance_id']}]}
            for i in kafka_list['instances'] if
            contains_keywords(i['name']) is True]
    return data


def get_redis_instances():
    redis_list = heyingyun_api(
        namespace='dcs',
        api_version='v2'
    )
    data = [{"dimensions": [{"name": "dcs_instance_name", "value": i['name']},
                            {"name": "dcs_instance_id", "value": i['instance_id']}]}
            for i in redis_list['instances'] if
            contains_keywords(i['name']) is True]
    return data


def get_rds_instances():
    rds_list = heyingyun_api(
        namespace='rds',
        api_version='v3'
    )
    data = [{"dimensions": [{"name": "rds_cluster_name", "value": i['name']},
                            {"name": "rds_cluster_id", "value": i['id']}]}
            for i in rds_list['instances'] if
            contains_keywords(i['name']) is True]
    return data


def get_elb_instances():
    elb_list = heyingyun_api(
        namespace='elb',
        api_version='v3',
        path='elb/loadbalancers'
    )
    data = [{"dimensions": [{"name": "lbaas_instance_name", "value": i['name']},
                            {"name": "lbaas_instance_id", "value": i['id']}]}
            for i in elb_list['loadbalancers'] if
            contains_keywords(i['name']) is True]
    return data


def merge_instances():
    kafka_instances = get_kafka_instances()
    redis_instances = get_redis_instances()
    rds_instances = get_rds_instances()
    elb_instances = get_elb_instances()
    instances = rds_instances + elb_instances + redis_instances + kafka_instances
    return instances
