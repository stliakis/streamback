import time

from streamback import Callback
from logging import INFO

import psutil
from ..utils import log, bytes_to_pretty_string


class ListenerStats(Callback):
    def __init__(self, interval=5):
        self.interval = interval
        self.last_tick = None

    def on_master_tick(self, listener_processes):
        if not self.last_tick:
            self.last_tick = time.time()
        else:
            if time.time() - self.last_tick < self.interval:
                return

        total_rss, total_vms = 0, 0

        memory_per_listener = {}
        for listener, process in listener_processes:
            process = psutil.Process(process.pid)
            memory_info = process.memory_info()
            total_rss += memory_info.rss
            listener_name = str(listener)
            memory_per_listener.setdefault(listener_name, {
                "rss": 0,
                "vms": 0,
                "procs": 0,
                "listener": listener_name
            })
            memory_per_listener[listener_name]["rss"] += memory_info.rss
            memory_per_listener[listener_name]["vms"] += memory_info.vms
            memory_per_listener[listener_name]["procs"] += 1

        stats = {
            "total_rss": total_rss,
            "total_vms": total_vms,
            "memory_per_listener": sorted(memory_per_listener.values(), key=lambda x: x["rss"], reverse=True)
        }

        self.on_stats(stats)

        self.last_tick = time.time()

    def on_stats(self, stats):
        for memory_per_listener in stats.get("memory_per_listener"):
            log(INFO, "STATS_OF_LISTENER[listener={listener},procs={procs},rss={rss},vms={vms}]".format(
                listener=memory_per_listener.get("listener"),
                procs=memory_per_listener.get("procs"),
                rss=bytes_to_pretty_string(memory_per_listener.get("rss")),
                vms=bytes_to_pretty_string(memory_per_listener.get("vms"))
            ))

        log(INFO,
            "TOTAL_STATS[total_rss={total_rss},total_vms={total_vms}]".format(
                total_rss=bytes_to_pretty_string(stats.get("total_rss")),
                total_vms=bytes_to_pretty_string(stats.get("total_vms"))))
