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
from unittest import mock

from airflow.jobs.triggerer_job_runner import TriggerRunner
from airflow.models.trigger import Trigger
from airflow.serialization.serialized_objects import BaseSerialization
import pendulum
import pytest

from airflow.triggers.base import TriggerEvent
from airflow.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger, PeriodicTrigger
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import utcnow


def test_input_validation():
    """
    Tests that the DateTimeTrigger validates input to moment arg, it should only accept datetime.
    """
    with pytest.raises(TypeError, match="Expected datetime.datetime type for moment. Got <class 'str'>"):
        DateTimeTrigger("2012-01-01T03:03:03+00:00")


def test_input_validation_tz():
    """
    Tests that the DateTimeTrigger validates input to moment arg, it shouldn't accept naive datetime.
    """

    moment = datetime.datetime(2013, 3, 31, 0, 59, 59)
    with pytest.raises(ValueError, match="You cannot pass naive datetimes"):
        DateTimeTrigger(moment)


def test_datetime_trigger_serialization():
    """
    Tests that the DateTimeTrigger correctly serializes its arguments
    and classpath.
    """
    moment = pendulum.instance(datetime.datetime(2020, 4, 1, 13, 0), pendulum.UTC)
    trigger = DateTimeTrigger(moment)
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.triggers.temporal.DateTimeTrigger"
    assert kwargs == {"moment": moment, "end_from_trigger": False}


def test_timedelta_trigger_serialization():
    """
    Tests that the TimeDeltaTrigger correctly serializes its arguments
    and classpath (it turns into a DateTimeTrigger).
    """
    trigger = TimeDeltaTrigger(datetime.timedelta(seconds=10))
    expected_moment = timezone.utcnow() + datetime.timedelta(seconds=10)
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.triggers.temporal.DateTimeTrigger"
    # We need to allow for a little time difference to avoid this test being
    # flaky if it runs over the boundary of a single second
    assert -2 < (kwargs["moment"] - expected_moment).total_seconds() < 2


@pytest.mark.parametrize(
    "tz, end_from_trigger",
    [
        (pendulum.timezone("UTC"), True),
        (pendulum.timezone("UTC"), False),  # only really need to test one
        (pendulum.timezone("Europe/Paris"), True),
        (pendulum.timezone("America/Toronto"), True),
    ],
)
@pytest.mark.asyncio
async def test_datetime_trigger_timing(tz, end_from_trigger):
    """
    Tests that the DateTimeTrigger only goes off on or after the appropriate
    time.
    """
    past_moment = pendulum.instance((timezone.utcnow() - datetime.timedelta(seconds=60)).astimezone(tz))
    future_moment = pendulum.instance((timezone.utcnow() + datetime.timedelta(seconds=60)).astimezone(tz))

    # Create a task that runs the trigger for a short time then cancels it
    trigger = DateTimeTrigger(future_moment, end_from_trigger=end_from_trigger)
    trigger_task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # It should not have produced a result
    assert trigger_task.done() is False
    trigger_task.cancel()

    # Now, make one waiting for en event in the past and do it again
    trigger = DateTimeTrigger(past_moment, end_from_trigger=end_from_trigger)
    trigger_task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    assert trigger_task.done() is True
    result = trigger_task.result()
    assert isinstance(result, TriggerEvent)
    expected_payload = TaskInstanceState.SUCCESS if end_from_trigger else past_moment
    assert result.payload == expected_payload


@mock.patch("airflow.triggers.temporal.timezone.utcnow")
@mock.patch("airflow.triggers.temporal.asyncio.sleep")
@pytest.mark.asyncio
async def test_datetime_trigger_mocked(mock_sleep, mock_utcnow):
    """
    Tests DateTimeTrigger with time and asyncio mocks
    """
    start_moment = utcnow()
    trigger_moment = start_moment + datetime.timedelta(seconds=30)

    # returns the mock 'current time'. The first 3 calls report the initial time
    mock_utcnow.side_effect = [
        start_moment,
        start_moment,
        start_moment,
        start_moment + datetime.timedelta(seconds=20),
        start_moment + datetime.timedelta(seconds=25),
        start_moment + datetime.timedelta(seconds=30),
    ]

    trigger = DateTimeTrigger(trigger_moment)
    gen = trigger.run()
    trigger_task = asyncio.create_task(gen.__anext__())
    await trigger_task
    mock_sleep.assert_awaited()
    assert mock_sleep.await_count == 2
    assert trigger_task.done() is True
    result = trigger_task.result()
    assert isinstance(result, TriggerEvent)
    assert result.payload == trigger_moment


def example_callback(target=5) -> bool:
    import random
    value = random.randint(0, 10)
    print(f"cond: value {value} < {target} target")
    if value < target:
        return True
    return False


"""
Hacky tests to confirm behaviour.  Nothing is mocked.
Proper tests next.
"""
@pytest.mark.asyncio
async def test_inefficient_non_mocked_periodic_trigger():
    trigger = PeriodicTrigger(
        interval_seconds=2,
        callback_objpath="tests.triggers.test_temporal.example_callback",
        callback_kwargs={"target": 3},
    )
    gen = trigger.run()
    trigger_task = asyncio.create_task(gen.__anext__())
    await trigger_task
    assert trigger_task.done() is True


def test_serialize_recreate_periodic_trigger():
    # Logic quickly copy pasted from providers.amazon.aws.triggers.test_serialization
    trigger = PeriodicTrigger(
        interval_seconds=10,
        callback_objpath="tests.triggers.test_temporal.example_callback",
        callback_kwargs={"param1":4, "param2":6},
    )
    # generate the DB object from the trigger
    trigger_db: Trigger = Trigger.from_object(trigger)

    # serialize/deserialize using the same method that is used when inserting in DB
    json_params = BaseSerialization.serialize(trigger_db.kwargs)
    retrieved_params = BaseSerialization.deserialize(json_params)

    # recreate a new trigger object from the data we would have in DB
    clazz = TriggerRunner().get_trigger_by_classpath(trigger_db.classpath)
    # noinspection PyArgumentList
    instance = clazz(**retrieved_params)

    # recreate a DB column object from the new trigger so that we can easily compare attributes
    trigger_db_2: Trigger = Trigger.from_object(instance)

    assert trigger_db.classpath == trigger_db_2.classpath
    assert trigger_db.kwargs == trigger_db_2.kwargs

