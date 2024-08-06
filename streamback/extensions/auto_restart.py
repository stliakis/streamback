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

    def on_master_tick(self, process_manager):
        if not self.last_tick:
            self.last_tick = time.time()
        else:
            if time.time() - self.last_tick < self.check_interval:
                return

        for topic_process in process_manager.get_topic_processes():
            if self.max_seconds:
                if topic_process.spawn_time < time.time() - self.max_seconds:
                    log(INFO,
                        "RESTARTING_PROCESS[topic={topic},reason=reached {restart_after_seconds} seconds lifetime]".format(
                            topic=topic_process.topic, restart_after_seconds=self.max_seconds))
                    topic_process.send_message(TopicProcessMessages.TERMINATE)

            if self.max_memory_mb:
                if not topic_process.is_process_alive():
                    continue

                process = psutil.Process(topic_process.process.pid)
                memory_info = process.memory_info()
                memory_usage_mb = memory_info.rss / 1024 / 1024

                log(INFO, "MEMORY_USAGE[topic={topic},rss={rss}]".format(
                    topic=topic_process.topic, rss=memory_usage_mb))

                if memory_usage_mb > self.max_memory_mb:
                    log(INFO, "RESTARTING_PROCESS[topic={topic},reason=exceeded {max_memory_mb} MB]".format(
                        topic=topic_process.topic, max_memory_mb=self.max_memory_mb))
                    topic_process.send_message(TopicProcessMessages.TERMINATE)

        self.last_tick = time.time()
