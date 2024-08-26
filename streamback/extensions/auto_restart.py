import time
from logging import INFO

import psutil

from streamback import Callback
from streamback.utils import log
from streamback.topic_process_messages import TopicProcessMessages


class AutoRestart(Callback):
    def __init__(self, max_seconds=None, max_memory_mb=None, check_interval=10):
        self.max_seconds = max_seconds
        self.max_memory_mb = max_memory_mb
        self.last_tick = None
        self.check_interval = check_interval

    def on_master_tick(self, streamback):
        if not self.last_tick:
            self.last_tick = time.time()
        else:
            if time.time() - self.last_tick < self.check_interval:
                return

        for listeners_process in streamback.process_manager.get_processes():
            if self.max_seconds:
                if listeners_process.spawn_time < time.time() - self.max_seconds:
                    log(INFO,
                        "RESTARTING_PROCESS[topics={topics},reason=reached {restart_after_seconds} seconds lifetime]".format(
                            topics=listeners_process.topics, restart_after_seconds=self.max_seconds))
                    listeners_process.send_message(TopicProcessMessages.TERMINATE)

            if self.max_memory_mb:
                if not listeners_process.is_process_alive():
                    continue

                process = psutil.Process(listeners_process.process.pid)
                memory_info = process.memory_info()
                memory_usage_mb = memory_info.rss / 1024 / 1024

                log(INFO, "MEMORY_USAGE[topics={topics},rss={rss}]".format(
                    topics=listeners_process.topics, rss=memory_usage_mb))

                if memory_usage_mb > self.max_memory_mb:
                    log(INFO, "RESTARTING_PROCESS[topics={topics},reason=exceeded {max_memory_mb} MB]".format(
                        topics=listeners_process.topics, max_memory_mb=self.max_memory_mb))
                    listeners_process.send_message(TopicProcessMessages.TERMINATE)

        self.last_tick = time.time()
