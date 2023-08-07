import time
from prometheus_client import start_http_server, REGISTRY
from ces_collector import CesCollector


def main():
    REGISTRY.register(CesCollector())
    start_http_server(8001)
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
