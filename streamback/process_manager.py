import multiprocessing
import signal
import sys
import time
from logging import INFO

from psutil import NoSuchProcess

from .topic_process_messages import TopicProcessMessages
from .utils import log, bytes_to_pretty_string


class ProcessManager(object):
    topic_process_managers = []
    concurrency_config = None

    def __init__(self, streamback, listeners, target):
        self.streamback = streamback
        self.concurrency_config = {}
        self.target = lambda *args, **kwargs: target(*args, **kwargs)
        self.listeners_per_topic = listeners
        if self.streamback.listen_only_for_topics:
            self.listeners_per_topic = {k: v for k, v in self.listeners_per_topic.items() if
                                        k in self.streamback.listen_only_for_topics}

    def get_available_process(self):
        return self.streamback.get_available_process()

    def spin(self):
        all_listeners = []
        for topic, listeners_of_topic in self.listeners_per_topic.items():
            all_listeners.extend(listeners_of_topic)

        for topic, listeners_of_topic in self.listeners_per_topic.items():
            concurrency_config = ConcurrencyConfig([])

            for listener in listeners_of_topic:
                if isinstance(listener.concurrency, int):
                    concurrency_config.update([[0, listener.concurrency]])
                elif isinstance(listener.concurrency, list):
                    if isinstance(listener.concurrency[0], list):
                        concurrency_config.update(listener.concurrency)
                    else:
                        min_concurrency = listener.concurrency[0]
                        max_concurrency = listener.concurrency[1]

                        concurrency = []

                        for index, i in enumerate(range(min_concurrency, max_concurrency + 1)):
                            concurrency.append([index * 1000, i])

                        concurrency_config.update(concurrency)
                else:
                    raise Exception("Invalid concurrency value")

            self.add_topic_process_manager(TopicProcessesManager(
                self,
                topic=topic,
                listeners=listeners_of_topic,
                target=self.target,
                concurrency_config=concurrency_config
            ))

        self.spawn_processes()

        def signal_handler(sig, frame):
            log(INFO, "PROCESS_MANAGER_MASTER_PROCESS_KILLED")
            self.terminate_all()
            self.streamback.close(all_listeners, reason="master process killed")
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while True:
                self.on_tick()

                for callback in self.streamback.get_callbacks():
                    callback.on_master_tick(self)

                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

    def spawn_processes(self):
        log(INFO,
            "PROCESS_MANAGER_INITIALIZING_PROCESS_MANAGERS[num={num}]".format(num=len(self.topic_process_managers)))
        for topic_process_manager in self.topic_process_managers:
            topic_process_manager.spawn()

    def on_tick(self):
        self.manage_processes()

    def manage_processes(self):
        for topic in self.topic_process_managers:
            topic.on_master_tick()

    def add_topic_process_manager(self, topic_process_manager):
        self.topic_process_managers.append(topic_process_manager)

    def terminate_all(self):
        for topic_process in self.topic_process_managers:
            topic_process.terminate()

    def send_message_to_all_processes(self, message):
        for topic_process in self.topic_process_managers:
            topic_process.send_message_to_all_processes(message)

    def get_topic_processes(self):
        processes = []
        for topic_process in self.topic_process_managers:
            processes.extend(topic_process.processes)
        return processes


class TopicProcessesManager(object):
    def __init__(self, process_manager, topic, listeners, target, concurrency_config):
        self.process_manager = process_manager
        self.topic = topic
        self.listeners = listeners
        self.target = lambda *args, **kwargs: target(*args, **kwargs)
        self.concurrency_config = concurrency_config
        self.processes = []
        self.last_rescale_time = time.time()

    def spawn(self):
        memory_usage = self.process_manager.streamback.get_current_memory_usage()
        messages_count, needed_number_of_processes = self.get_needed_concurrency()
        current_number_of_processes = len(self.processes)
        processes_rescale_number = needed_number_of_processes - current_number_of_processes

        # if needed_processes_number != 0:
        log(INFO,
            "CHECKING_PROCESSES[topic={topic},scaling={scaling},messages={messages_count},current_memory={current_memory_usage},needed={needed},current={current},change={change}]".format(
                topic=self.topic,
                scaling=self.concurrency_config.scaling,
                needed=needed_number_of_processes,
                current=current_number_of_processes,
                messages_count=messages_count,
                change=processes_rescale_number,
                current_memory_usage=bytes_to_pretty_string(memory_usage)
            ))

        if processes_rescale_number > 0:
            if self.is_memory_usage_reached():
                log(INFO,
                    "MEMORY_USAGE_REACHED[topic={topic},current_memory={current_memory_usage},max_memory_usage={max_memory_usage} MB]".format(
                        topic=self.topic,
                        current_memory_usage=bytes_to_pretty_string(memory_usage),
                        max_memory_usage=self.get_max_memory_usage_mb()
                    ))
                return

            for i in range(processes_rescale_number):
                process = TopicProcess(self.topic, self.listeners, self.target)
                self.processes.append(process)
                process.spawn()
                log(INFO, "SPAWNED_PROCESS[topic={topic},listeners={listeners}]".format(topic=self.topic,
                                                                                        listeners=self.listeners))
        elif processes_rescale_number < 0:
            for i in range(abs(processes_rescale_number)):
                process = self.processes.pop()
                if process.get_seconds_alive() > self.process_manager.streamback.rescale_min_process_ttl:
                    process.terminate()
                    log(INFO, "TERMINATED_PROCESS[topic={topic},reason=rescaling]".format(topic=self.topic))
                else:
                    self.processes.append(process)

    def on_master_tick(self):
        for process in self.processes:
            process.on_master_tick()

        if time.time() - self.last_rescale_time > self.process_manager.streamback.rescale_interval:
            self.spawn()
            self.last_rescale_time = time.time()

    def get_max_memory_usage_mb(self):
        return self.process_manager.streamback.rescale_max_memory_mb

    def is_memory_usage_reached(self):
        max_allowed_usage = self.get_max_memory_usage_mb()
        if not max_allowed_usage:
            return False

        max_memory_usage = max_allowed_usage * 1024 * 1024

        if not max_memory_usage:
            return False

        memory_usage = self.process_manager.streamback.get_current_memory_usage()
        print("max_memory:", max_memory_usage)
        print("current:", memory_usage)

        return memory_usage > max_memory_usage

    def check_max_concurrency_setting(self, num_of_procs):
        if num_of_procs > 50:
            log(INFO, "MAX_CONCURRENCY_SETTING_POSSIBLE_MISCONFIG[topic={topic},num_of_procs={num_of_procs}]".format(
                topic=self.topic,
                num_of_procs=num_of_procs
            ))
            return 50
        return num_of_procs

    def get_needed_concurrency(self):
        streamback = self.process_manager.streamback
        messages_count = streamback.main_stream.get_message_count_in_topic(
            streamback.get_topic_real_name(self.topic)
        )

        for concurrency in self.concurrency_config.scaling:
            messages_threshold = concurrency[0]
            num_of_procs = self.check_max_concurrency_setting(concurrency[1])

            if messages_count <= messages_threshold:
                return messages_count, num_of_procs

        concurrency = self.check_max_concurrency_setting(self.concurrency_config.scaling[-1][1])

        return messages_count, concurrency

    def terminate_all(self):
        for topic_process in self.processes:
            topic_process.terminate()

    def send_message_to_all_processes(self, message):
        for topic_process in self.processes:
            topic_process.send_message(message)


class TopicProcess(object):
    main_pipe = None
    child_pipe = None
    process = None
    spawn_time = None
    terminated = False

    def __init__(self, topic, listeners, target):
        self.topic = topic
        self.listeners = listeners
        self.target = lambda *args, **kwargs: target(*args, **kwargs)

    def spawn(self):
        self.terminated = False

        main_pipe, child_pipe = multiprocessing.Pipe()
        self.child_pipe = child_pipe
        self.main_pipe = main_pipe
        self.process = multiprocessing.Process(
            target=self.target,
            args=(
                child_pipe,
                self.topic,
                self.listeners,
            ),
        )
        self.process.start()
        self.spawn_time = time.time()

    def is_process_alive(self):
        try:
            return self.process.is_alive()
        except NoSuchProcess:
            return False

    def get_seconds_alive(self):
        return time.time() - self.spawn_time

    def on_master_tick(self):
        if not self.terminated and not self.is_process_alive():
            log(INFO, "PROCESS_IS_DEAD[topic={topic}]".format(topic=self.topic))
            log(INFO, "RESPAWNING_PROCESS[topic={topic}]".format(topic=self.topic))
            self.spawn()

    def send_message(self, message):
        self.main_pipe.send(message)

    def terminate(self):
        self.terminated = True
        self.send_message(TopicProcessMessages.TERMINATE)
        self.process.join()


class ConcurrencyConfig(object):
    def __init__(self, scaling):
        self.scaling = list(sorted(scaling, key=lambda x: x[0]))

    def update(self, scaling):
        ## TODO, merge the scaling with a better way
        self.scaling = scaling

    def __repr__(self):
        return "<ConcurrencyConfig scaling=%s>" % (
            self.scaling
        )
