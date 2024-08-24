import json
import os.path
import time
from datetime import datetime, timedelta
from logging import INFO

from .utils import log


class Scheduler(object):
    def __init__(self, state):
        self.tasks = []
        self.state = state

    def add_task(self, name, when, then, args=None, description=None):
        self.tasks.append(SchedulerTask(
            name=name,
            cron_checker=CronChecker(name, pattern=when, state=self.state),
            then=then,
            args=args,
            description=description
        ))

    def execute(self, streamback):
        for task in self.tasks:
            task.execute_if_needed(streamback)
        self.state.save()


class SchedulerTask(object):
    def __init__(self, name, cron_checker, then, args=None, description=None):
        self.name = name
        self.then = then
        self.args = args or {}
        self.description = description
        self.cron_checker = cron_checker

    def execute_if_needed(self, streamback):
        valid = self.cron_checker.check(datetime.now())
        if valid:
            log(INFO, "STREAMBACK_SCHEDULER_EXECUTE_TASK[task={task}]".format(
                task=self
            ))
            streamback.send(self.then, self.args)

    def __repr__(self):
        return "SchedulerTask[name={name},when={when},then={then},args={args},description={description}]".format(
            name=self.name,
            then=self.then,
            when=self.cron_checker,
            args=self.args,
            description=self.description
        )


class SchedulerState(object):
    def __init__(self, filepath, state_ttl=3600):
        self.filepath = filepath
        self.executions = []
        self.state_ttl = state_ttl
        self.is_dirty = False
        self.read()

    def add_execution(self, id):
        self.executions.append({
            "id": id,
            "time": time.time()
        })
        self.is_dirty = True

    def contains_execution(self, id):
        return id in [i.get("id") for i in self.executions]

    def read(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, "r") as file:
                data = json.load(file)

                self.executions = data.get("executions")
        else:
            self.executions = []
            self.save()

    def cleanup_executions(self):
        keep_until = time.time() - self.state_ttl
        self.executions = [execution for execution in self.executions if execution.get("time") > keep_until]

    def save(self):
        self.cleanup_executions()

        if self.is_dirty:
            t = time.time()
            with open(self.filepath, "w") as file:
                json.dump({
                    "executions": self.executions
                }, file)
                self.is_dirty = False


class CronChecker(object):
    def __init__(self, name, pattern, state, seconds_in_the_past_to_check_at_startup=60):
        self.pattern = pattern
        self.name = name
        self.state = state
        self.seconds_in_the_past_to_check_at_startup = seconds_in_the_past_to_check_at_startup
        self.found_matches = []
        self.last_check = None

    def __repr__(self):
        return "CronChecker[pattern={pattern}]".format(
            pattern=self.pattern
        )

    def check(self, current_time):
        if not self.last_check:
            seconds_past = self.seconds_in_the_past_to_check_at_startup
        else:
            seconds_past = int(time.time() - self.last_check)

        for i in range(seconds_past):
            match = self.check_datetime(current_time - timedelta(seconds=i))
            if match:
                self.last_check = time.time()
                return match
        return False

    def check_datetime(self, current_time):
        timestamp = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        execution_id = "{}-{}".format(
            self.name, timestamp
        )

        if self.state.contains_execution(execution_id):
            return False

        parts = self.pattern.strip().split()

        if len(parts) == 6:
            sec, min, hour, day, month, day_of_week = parts
        else:
            sec = "0"
            min, hour, day, month, day_of_week = parts

        def match(part, value, max_value):
            if part == "*":
                return True
            elif "*/" in part:
                step = int(part.split("*/")[1])
                return value % step == 0
            elif "," in part:
                return any(value == int(p) for p in part.split(","))
            elif "-" in part:
                start, end = map(int, part.split("-"))
                return start <= value <= end
            else:
                return int(part) == value

        matches = (match(sec, current_time.second, 60) and
                   match(min, current_time.minute, 60) and
                   match(hour, current_time.hour, 24) and
                   match(day, current_time.day, 31) and
                   match(month, current_time.month, 12) and
                   match(day_of_week, current_time.weekday(), 7))

        if matches:
            self.state.add_execution(execution_id)
            return execution_id

        return False
