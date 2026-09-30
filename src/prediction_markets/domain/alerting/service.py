"""Route incidents through notification channels and timeout escalation.

Responsibilities
----------------
- Decide which registered recipients are notified for an incident.
- Decide the severity implied by elapsed time since the incident opened.

Notes
-----
- A `Recipient` models one destination on one channel, so the policy
  filters by the recipient's own channel instead of reusing a single
  address across unrelated channels.
- `Channel.OTHER` is never routed by this policy.
- Escalation decides a severity; it does not mutate the incident.
"""

from collections.abc import Iterable
from datetime import timedelta

from prediction_markets.domain.alerting.entities import Incident
from prediction_markets.domain.alerting.enums import Channel, IncidentStatus, Severity
from prediction_markets.domain.alerting.value_objects import Recipient
from prediction_markets.domain.shared.value_objects import Timestamp

CHANNELS_BY_SEVERITY: dict[Severity, frozenset[Channel]] = {
    Severity.CRITICAL: frozenset({Channel.SMS, Channel.EMAIL, Channel.PUSH}),
    Severity.MAJOR: frozenset({Channel.EMAIL, Channel.PUSH}),
    Severity.MINOR: frozenset({Channel.EMAIL, Channel.PUSH}),
    Severity.WARNING: frozenset({Channel.EMAIL, Channel.PUSH}),
    Severity.INFORMATIONAL: frozenset({Channel.EMAIL}),
    Severity.UNKNOWN: frozenset({Channel.EMAIL, Channel.PUSH}),
}
"""Channels enabled for each severity.

Invariants
----------
- Every `Severity` member must have an entry; adding a severity without
  updating this table raises `KeyError` at routing time, surfacing the
  missing policy decision instead of silently dropping notifications.
- `Channel.EMAIL` is enabled at every severity level.
- `Severity.UNKNOWN` fails open to email.
"""

ESCALATION_TIMEOUTS: dict[
    tuple[IncidentStatus, Severity],
    tuple[timedelta, Severity],
] = {
    (IncidentStatus.OPEN, Severity.MAJOR): (timedelta(hours=4), Severity.CRITICAL),
    (IncidentStatus.OPEN, Severity.MINOR): (timedelta(days=1), Severity.MAJOR),
    (IncidentStatus.ACKNOWLEDGED, Severity.MAJOR): (timedelta(hours=4),Severity.CRITICAL),
    (IncidentStatus.ACKNOWLEDGED, Severity.MINOR): (timedelta(days=1),Severity.MAJOR),
    (IncidentStatus.IN_PROGRESS, Severity.MAJOR): (timedelta(hours=2), Severity.CRITICAL),
    (IncidentStatus.IN_PROGRESS, Severity.MINOR): (timedelta(hours=12), Severity.MAJOR),
}
"""Timeout and next severity for each (status, severity) pair.

A missing key keeps the current severity. `IN_PROGRESS` uses shorter
timeouts than `OPEN` and `ACKNOWLEDGED`.
"""


class NotificationPolicy:
    """Select the recipients to notify for an incident.

    Attributes
    ----------
    recipients : frozenset[Recipient]
        Registered notification destinations.

    Notes
    -----
    - A recipient is notified only through its own channel, and only
      when the incident severity routes to that channel. Reaching one
      person on several channels requires one `Recipient` per channel.
    """

    def __init__(self, recipients: Iterable[Recipient]) -> None:
        """
        Parameters
        ----------
        recipients : Iterable[Recipient]
            Destinations available for notification, stored as an
            immutable set.
        """
        self.recipients = frozenset(recipients)

    def targets_for(self, incident: Incident) -> set[Recipient]:
        """Return the recipients to notify for the incident's severity.

        Parameters
        ----------
        incident : Incident
            Incident whose severity drives the escalation level.

        Returns
        -------
        set[Recipient]
            Recipients whose own channel is enabled for the incident's
            severity. Empty when no registered recipient matches.

        Raises
        ------
        KeyError
            If the incident severity has no entry in
            `CHANNELS_BY_SEVERITY`.
        """
        channels = CHANNELS_BY_SEVERITY[incident.severity]
        return {
            recipient
            for recipient in self.recipients
            if recipient.channel in channels
        }
    
    def targets_for_notification(self, severity: Severity) -> set[Recipient]:
        """Return recipients to notify for a severity without an incident.

        Parameters
        ----------
        severity
            Severity used to select enabled notification channels.

        Returns
        -------
        set[Recipient]
            Recipients whose channel is enabled for the severity.
        """
        channels = CHANNELS_BY_SEVERITY[severity]
        return {
            recipient
            for recipient in self.recipients
            if recipient.channel in channels
        }


class EscalationPolicy:
    """Decide the severity implied by elapsed time since the incident opened.

    Notes
    -----
    - Does not call `Incident.change_severity`. The caller applies the
      returned value when it differs from `incident.severity`.
    """

    def severity_after_timeout(
        self,
        incident: Incident,
        at: Timestamp,
    ) -> Severity:
        """Return the severity after applying the matching timeout rule.

        Parameters
        ----------
        incident
            Incident whose status, severity, and `opened_at` are evaluated.
        at
            Instant used as the comparison clock.

        Returns
        -------
        Severity
            Next severity when `(status, severity)` has a timeout that
            has elapsed; otherwise the incident's current severity.
        """
        rule = ESCALATION_TIMEOUTS.get((incident.status, incident.severity))
        if rule is None:
            return incident.severity

        timeout, next_severity = rule
        if incident.opened_at + timeout < at:
            return next_severity

        return incident.severity
