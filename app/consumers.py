"""Kafka consumers for patient-search-service.

One handler per subscribed topic. Handlers are best-effort logging plus
audit — services override this file to implement real cross-domain behavior.
"""
from __future__ import annotations

import logging

from healthcare_common.audit import emit_audit

log = logging.getLogger("patient-search-service.consumers")


def register(svc) -> None:
    bus = svc.bus

    @bus.on("patient.created")
    def _on_patient_created(envelope: dict) -> None:
        log.info("patient-search-service: received patient.created id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.patient.created", actor="system:patient-search-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.updated")
    def _on_patient_updated(envelope: dict) -> None:
        log.info("patient-search-service: received patient.updated id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.patient.updated", actor="system:patient-search-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.merged")
    def _on_patient_merged(envelope: dict) -> None:
        log.info("patient-search-service: received patient.merged id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.patient.merged", actor="system:patient-search-service",
                   target=None, details={"envelope_id": envelope.get("id")})

