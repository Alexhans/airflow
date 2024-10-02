# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import asyncio
import datetime
from typing import Any, AsyncIterator, Dict, Optional

from airflow.utils.module_loading import import_string
import pendulum

from airflow.triggers.base import BaseTrigger, TaskSuccessEvent, TriggerEvent
from airflow.utils import timezone


class DateTimeTrigger(BaseTrigger):
    """
    Trigger based on a datetime.

    A trigger that fires exactly once, at the given datetime, give or take
    a few seconds.

    The provided datetime MUST be in UTC.

    :param moment: when to yield event
    :param end_from_trigger: whether the trigger should mark the task successful after time condition
        reached or resume the task after time condition reached.
    """

    def __init__(self, moment: datetime.datetime, *, end_from_trigger: bool = False) -> None:
        super().__init__()
        if not isinstance(moment, datetime.datetime):
            raise TypeError(f"Expected datetime.datetime type for moment. Got {type(moment)}")
        # Make sure it's in UTC
        elif moment.tzinfo is None:
            raise ValueError("You cannot pass naive datetimes")
        else:
            self.moment: pendulum.DateTime = timezone.convert_to_utc(moment)
        self.end_from_trigger = end_from_trigger

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.triggers.temporal.DateTimeTrigger",
            {"moment": self.moment, "end_from_trigger": self.end_from_trigger},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Loop until the relevant time is met.

        We do have a two-phase delay to save some cycles, but sleeping is so
        cheap anyway that it's pretty loose. We also don't just sleep for
        "the number of seconds until the time" in case the system clock changes
        unexpectedly, or handles a DST change poorly.
        """
        # Sleep in successively smaller increments starting from 1 hour down to 10 seconds at a time
        self.log.info("trigger starting")
        for step in 3600, 60, 10:
            seconds_remaining = (self.moment - pendulum.instance(timezone.utcnow())).total_seconds()
            while seconds_remaining > 2 * step:
                self.log.info("%d seconds remaining; sleeping %s seconds", seconds_remaining, step)
                await asyncio.sleep(step)
                seconds_remaining = (self.moment - pendulum.instance(timezone.utcnow())).total_seconds()
        # Sleep a second at a time otherwise
        while self.moment > pendulum.instance(timezone.utcnow()):
            self.log.info("sleeping 1 second...")
            await asyncio.sleep(1)
        if self.end_from_trigger:
            self.log.info("Sensor time condition reached; marking task successful and exiting")
            yield TaskSuccessEvent()
        else:
            self.log.info("yielding event with payload %r", self.moment)
            yield TriggerEvent(self.moment)


class TimeDeltaTrigger(DateTimeTrigger):
    """
    Create DateTimeTriggers based on delays.

    Subclass to create DateTimeTriggers based on time delays rather
    than exact moments.

    While this is its own distinct class here, it will serialise to a
    DateTimeTrigger class, since they're operationally the same.

    :param delta: how long to wait
    :param end_from_trigger: whether the trigger should mark the task successful after time condition
        reached or resume the task after time condition reached.
    """

    def __init__(self, delta: datetime.timedelta, *, end_from_trigger: bool = False) -> None:
        super().__init__(moment=timezone.utcnow() + delta, end_from_trigger=end_from_trigger)


class PeriodicTrigger(BaseTrigger):
    """
    A trigger that fires on a recurring basis until the callback condition is met.

    This would be fantastic those looking to replace their custom sensors in reschedule mode with
    something that is fully handled by the trigger (Ideal for cases where the waits are long, such
    as polling for data completeness).

    This is example is periodic (fixed time) for simplicity but supporting backoff rates should
    be possible. Whether to make that 2 different classes or PeriodicTrigger being a specific
    version of BackoffRateTrigger is up to discussion.  Down the line, this could be implemented so
    that the baes sensor itself has deferrable=True or mode=deferrable support (which should be incomptabile with
    mode=poke|reschedule).

    The trigger follows the rule of no state and being serializable but one of the main things
    to get feedback on is the serialization of the callback method.

    Probably the most important thing to get feedback on is the serialziation and deserialization of
    the callback object.  I looked into airflow.jobs.triggerer_job_runner code to see how the deserialization
    of the class instances was done TriggerRunner.update_triggers calls self.get_trigger_by_classpath which
    uses the util function import_string (and does caching).
    """
    def __init__(self, callback_objpath: str, interval_seconds: int, callback_kwargs: Optional[Dict] = None) -> None:
        super().__init__()
        self.callback_objpath = callback_objpath
        self.interval_seconds = interval_seconds
        self.callback: callable[..., bool] = import_string(self.callback_objpath)
        self.callback_kwargs=callback_kwargs or dict() # or (x if x is not None else x)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.triggers.temporal.PeriodicTrigger",
            {
                "callback_objpath": self.callback_objpath,
                "interval_seconds": self.interval_seconds,
                "callback_kwargs": self.callback_kwargs,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        # Indirection leftover from a more complex real scenario.  Could be one function.
        await self.wait_for_completion()
        yield TaskSuccessEvent()

    async def wait_for_completion(self):
        """Loop until callback function returns True.  Similar to a Sensor
            If False, wait a specific amount of time (which eventually could contain
            backoff rates and more)
        """
        while True:
            is_complete = self.callback(**self.callback_kwargs)
            if not is_complete:
                self.log.warning("Sleeping %d seconds", self.interval_seconds)
                await asyncio.sleep(self.interval_seconds)
            else:
                self.log.warning("Condition met")
                return is_complete

