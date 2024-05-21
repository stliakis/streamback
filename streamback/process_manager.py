import multiprocessing
import signal
import sys
import time
from logging import INFO

from psutil import NoSuchProcess

from .topic_process_messages import TopicProcessMessages
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
                                                                                                     listeners_of_topic))
                                                                                       for topic, listeners_of_topic in
                                                                                       listeners.items()])))

        for topic, listeners_of_topic in listeners.items():
            for i in range(listener_procs_per_topic.get(topic)):
                topic_process = TopicProcess(topic=topic, listeners=listeners_of_topic, target=target)
                self.add_topic_process(
                    topic_process
                )
                topic_process.spawn()

        def signal_handler(sig, frame):
            log(INFO, "PROCESS_MANAGER_MASTER_PROCESS_KILLED")
            self.terminate_all()
            streamback.close(all_listeners)
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while True:
                for callback in streamback.get_callbacks():
                    callback.on_master_tick(self)

                for topic_process in self.topic_processes:
                    topic_process.on_master_tick()

                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

    def add_topic_process(self, topic_process):
        self.topic_processes.append(topic_process)

    def terminate_all(self):
        for topic_process in self.topic_processes:
            topic_process.terminate()

    def send_message_to_all_processes(self, message):
        for topic_process in self.topic_processes:
            topic_process.send_message(message)


class TopicProcess(object):
    main_pipe = None
    child_pipe = None
    process = None
    spawn_time = None

    def __init__(self, topic, listeners, target):
        self.topic = topic
        self.listeners = listeners
        self.target = lambda *args, **kwargs: target(*args, **kwargs)

    def spawn(self):
        main_pipe, child_pipe = multiprocessing.Pipe()
        self.child_pipe = child_pipe
        self.main_pipe = main_pipe
        self.process = multiprocessing.Process(target=self.target,
                                               args=(child_pipe, self.topic, self.listeners,))
        self.process.start()
        self.spawn_time = time.time()

    def is_process_alive(self):
        try:
            return self.process.is_alive()
        except NoSuchProcess:
            return False

    def on_master_tick(self):
        if not self.is_process_alive():
            log(INFO, "PROCESS_IS_DEAD[topic={topic}]".format(topic=self.topic))
            log(INFO, "RESPAWNING_PROCESS[topic={topic}]".format(topic=self.topic))
            self.spawn()

    def send_message(self, message):
        self.main_pipe.send(message)

    def terminate(self):
        self.send_message(TopicProcessMessages.TERMINATE)
        self.process.join()
