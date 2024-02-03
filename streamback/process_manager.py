import multiprocessing
import signal
import sys
import time
from logging import INFO

from .utils import log


class ProcessManager(object):
    topic_processes = []

    def spin(self, streamback, target, listeners):
        all_listeners = []
        for topic, listeners_of_topic in listeners.items():
            all_listeners.extend(listeners_of_topic)

        all_listener_procs_count = int(sum([listener.concurrency for listener in all_listeners]))
        listener_procs_per_topic = {topic: int(sum(listener.concurrency for listener in listeners_of_topic)) for
                                    topic, listeners_of_topic
                                    in listeners.items()}

        log(INFO,
            "PROCESS_MANAGER_STARTING_PROCESSES[num={num},topics={topics}]".format(num=all_listener_procs_count,
                                                                                   topics=",".join([
                                                                                       "%s[procs=%s,listeners=%s]" % (
                                                                                           topic,
                                                                                           listener_procs_per_topic.get(
                                                                                               topic),
                                                                                           ", ".join(str(listener) for
                                                                                                     listener in
                                                                                                     listeners))
                                                                                       for topic, listeners in
                                                                                       listeners.items()])))

        for topic, listeners in listeners.items():
            for i in range(listener_procs_per_topic.get(topic)):
                process = multiprocessing.Process(target=target, args=(topic, listeners,))
                process.start()
                self.add_topic_process(
                    TopicProcess(topic=topic, listeners=listeners, process=process)
                )

        def signal_handler(sig, frame):
            log(INFO, "PROCESS_MANAGER_MASTER_PROCESS_KILLED")
            self.terminate_all()
            streamback.close()
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while True:
                for callback in streamback.get_callbacks():
                    callback.on_master_tick(self)
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

    def add_topic_process(self, topic_process):
        self.topic_processes.append(topic_process)

    def terminate_all(self):
        for topic_process in self.topic_processes:
            topic_process.terminate()


class TopicProcess(object):
    def __init__(self, topic, listeners, process):
        self.topic = topic
        self.listeners = listeners
        self.process = process

    def terminate(self):
        self.process.terminate()
        self.process.join()
