from __future__ import annotations

from importlib.metadata import version

import pytest
from sagaz import Saga, SagaConfig, action, compensate

from app.order.workflow import create_order_processing_saga


class RecordingSagaStorage:
    """Small spy for the SagaStorage calls made by Sagaz 1.5.0."""

    def __init__(self) -> None:
        self.writes: list[dict] = []
        self.loads: list[str] = []

    async def load_saga_state(self, saga_id: str) -> dict | None:
        self.loads.append(saga_id)
        return None

    async def save_saga_state(
        self,
        saga_id,
        saga_name,
        status,
        steps,
        context,
        metadata=None,
    ) -> None:
        self.writes.append(
            {
                "saga_id": saga_id,
                "saga_name": saga_name,
                "status": status.value,
                "steps": steps,
                "context": dict(context),
                "metadata": metadata,
            }
        )


class SuccessfulPersistenceProbe(Saga):
    saga_name = "persistence-probe-success"

    @action("record_side_effect")
    async def record_side_effect(self, context: dict) -> dict:
        return {"effect_reference": "effect-123"}


class FailingPersistenceProbe(Saga):
    saga_name = "persistence-probe-compensation"
    compensation_executed = False

    @action("reserve_inventory")
    async def reserve_inventory(self, context: dict) -> dict:
        return {"reservation_id": "reservation-123"}

    @compensate("reserve_inventory")
    async def release_inventory(self, context: dict) -> None:
        type(self).compensation_executed = True

    @action("charge_payment", depends_on=["reserve_inventory"])
    async def charge_payment(self, context: dict) -> dict:
        raise RuntimeError("simulated payment decline")


@pytest.mark.asyncio
async def test_pinned_sagaz_version_persists_only_top_level_start_and_completion():
    assert version("sagaz") == "1.5.0"
    storage = RecordingSagaStorage()
    config = SagaConfig(
        storage=storage,
        logging=False,
        metrics=False,
        tracing=False,
    )
    saga = SuccessfulPersistenceProbe(config=config)

    result = await saga.run({"order_id": 41}, saga_id="probe-success")

    assert result["effect_reference"] == "effect-123"
    assert [record["status"] for record in storage.writes] == [
        "executing",
        "completed",
    ]
    assert [record["steps"] for record in storage.writes] == [[], []]
    assert storage.writes[0]["context"]["order_id"] == 41
    assert storage.writes[1]["context"]["effect_reference"] == "effect-123"


@pytest.mark.asyncio
async def test_pinned_sagaz_persists_no_compensation_progress():
    storage = RecordingSagaStorage()
    config = SagaConfig(
        storage=storage,
        logging=False,
        metrics=False,
        tracing=False,
    )
    FailingPersistenceProbe.compensation_executed = False
    saga = FailingPersistenceProbe(config=config)

    with pytest.raises(RuntimeError, match="simulated payment decline"):
        await saga.run({"order_id": 42}, saga_id="probe-compensation")

    assert FailingPersistenceProbe.compensation_executed is True
    assert [record["status"] for record in storage.writes] == [
        "executing",
        "rolled_back",
    ]
    assert [record["steps"] for record in storage.writes] == [[], []]
    assert storage.writes[-1]["context"]["__reserve_inventory_compensated"] is True


@pytest.mark.asyncio
async def test_current_order_workflow_uses_in_memory_sagaz_storage_by_default():
    saga, _tracker = create_order_processing_saga()

    assert type(saga._config.storage).__name__ == "InMemorySagaStorage"

@pytest.mark.asyncio
async def test_reusing_a_saga_id_does_not_load_or_resume_the_existing_record():
    storage = RecordingSagaStorage()
    config = SagaConfig(
        storage=storage,
        logging=False,
        metrics=False,
        tracing=False,
    )
    executions = 0

    class ReplayedSideEffectProbe(Saga):
        saga_name = "persistence-probe-replay"

        @action("record_side_effect")
        async def record_side_effect(self, context: dict) -> dict:
            nonlocal executions
            executions += 1
            return {"execution_count": executions}

    first = ReplayedSideEffectProbe(config=config)
    second = ReplayedSideEffectProbe(config=config)

    await first.run({"order_id": 43}, saga_id="same-saga-id")
    resumed = await second.run({"order_id": 43}, saga_id="same-saga-id")

    assert storage.loads == []
    assert executions == 2
    assert resumed["execution_count"] == 2
    assert [record["status"] for record in storage.writes] == [
        "executing",
        "completed",
        "executing",
        "completed",
    ]


@pytest.mark.asyncio
async def test_sagaz_swallows_state_storage_errors_and_continues_execution():
    class UnavailableStorage:
        async def save_saga_state(self, *args, **kwargs) -> None:
            raise ConnectionError("storage unavailable")

    config = SagaConfig(
        storage=UnavailableStorage(),
        logging=False,
        metrics=False,
        tracing=False,
    )
    saga = SuccessfulPersistenceProbe(config=config)

    result = await saga.run({"order_id": 44}, saga_id="storage-unavailable")

    assert result["effect_reference"] == "effect-123"

