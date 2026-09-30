"""Expose authenticated manual alerting lifecycle actions over HTTP.

Responsibilities
----------------
- Validate operational identifiers and invoke the inbound alerting port.
- Map missing resources and invalid lifecycle transitions to HTTP errors.
"""

from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from prediction_markets.api.dependencies import get_alerting_port
from prediction_markets.api.auth import require_trading_key
from prediction_markets.domain.alerting.ports import AlertingPort
from prediction_markets.domain.alerting.value_objects import DeliveryID, IncidentID
from prediction_markets.domain.shared.value_objects import Timestamp

router = APIRouter(
    prefix="/alerts",
    tags=["alerting"],
    dependencies=[Depends(require_trading_key)],
)


def _apply(action: Callable[[], None], status: str) -> dict[str, str]:
    """Run one alerting command and normalize domain errors."""
    try:
        action()
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error.args[0])) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return {"status": status}


@router.post("/incidents/{incident_id}/acknowledge")
def acknowledge_incident(
    incident_id: str,
    alerting: Annotated[AlertingPort, Depends(get_alerting_port)],
) -> dict[str, str]:
    """Acknowledge an open incident."""
    return _apply(
        lambda: alerting.acknowledge_incident(
            IncidentID(incident_id),
            Timestamp.now(),
        ),
        "acknowledged",
    )


@router.post("/incidents/{incident_id}/in-progress")
def mark_incident_in_progress(
    incident_id: str,
    alerting: Annotated[AlertingPort, Depends(get_alerting_port)],
) -> dict[str, str]:
    """Mark an acknowledged incident as actively investigated."""
    return _apply(
        lambda: alerting.mark_incident_in_progress(
            IncidentID(incident_id),
            Timestamp.now(),
        ),
        "in_progress",
    )


@router.post("/deliveries/{delivery_id}/retry")
def retry_notification(
    delivery_id: str,
    alerting: Annotated[AlertingPort, Depends(get_alerting_port)],
) -> dict[str, str]:
    """Return one failed delivery to the pending outbox."""
    return _apply(
        lambda: alerting.retry_notification(
            DeliveryID(delivery_id),
            Timestamp.now(),
        ),
        "pending",
    )
