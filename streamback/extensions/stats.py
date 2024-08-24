import time

from streamback import Callback
from logging import INFO

import psutil
from ..utils import log, bytes_to_pretty_string

total_consumed_messages = 0


class ListenerStats(Callback):
    def __init__(self, interval=5):
        self.interval = interval
        self.last_tick = None

    def on_master_tick(self, streamback):
        if not self.last_tick:
            self.last_tick = time.time()
        else:
            if time.time() - self.last_tick < self.interval:
                return

        total_rss, total_vms = 0, 0

        memory_per_topic = {}
        for topic_process in streamback.process_manager.get_processes():
            if not topic_process.is_process_alive():
                log(INFO, "PROCESS_IS_DEAD[topic={topic}]".format(topic=topic_process.topics))
                continue

            process = psutil.Process(topic_process.process.pid)
            memory_info = process.memory_info()
            total_rss += memory_info.rss
            topic_name = str(topic_process.topics)
            memory_per_topic.setdefault(topic_name, {
                "rss": 0,
                "vms": 0,
                "procs": 0,
                "topic": topic_name
            })
            memory_per_topic[topic_name]["rss"] += memory_info.rss
            memory_per_topic[topic_name]["vms"] += memory_info.vms
            memory_per_topic[topic_name]["procs"] += 1

        stats = {
            "total_rss": total_rss,
            "total_vms": total_vms,
            "memory_per_topic": sorted(memory_per_topic.values(), key=lambda x: x["rss"], reverse=True)
        }

        self.on_stats(stats)

        self.last_tick = time.time()

    def on_stats(self, stats):
        for memory_per_topic in stats.get("memory_per_topic"):
            log(INFO, "STATS_OF_TOPIC[topic={topic},procs={procs},rss={rss},vms={vms}]".format(
                topic=memory_per_topic.get("topic"),
                procs=memory_per_topic.get("procs"),
                rss=bytes_to_pretty_string(memory_per_topic.get("rss")),
                vms=bytes_to_pretty_string(memory_per_topic.get("vms"))
            ))

        log(INFO,
            "TOTAL_STATS[total_rss={total_rss},total_vms={total_vms}]".format(
                total_rss=bytes_to_pretty_string(stats.get("total_rss")),
                total_vms=bytes_to_pretty_string(stats.get("total_vms"))))
