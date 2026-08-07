"""Kafka consumers for patient-search-service.

One handler per subscribed topic. Real handlers write to this service's own
database and/or publish follow-up events; stub handlers just log + audit.
"""
from __future__ import annotations

import logging

from psycopg.types.json import Json

from healthcare_common.audit import emit_audit

log = logging.getLogger("patient-search-service.consumers")

TABLE = "patient_search"


def register(svc) -> None:
    bus = svc.bus
    db = svc.db
    clients = svc.clients

    @bus.on("patient.created")
    def _on_patient_created(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    pid = data.get("id") or data.get("patient_id")
                    if not pid: return
                    # Idempotent snapshot: keep the latest name/dob for this patient in our own store.
                    existing = db.query_one(
                        f"SELECT id FROM {TABLE} WHERE data->>'patient_id' = %s",
                        (str(pid),),
                    )
                    snapshot = {
                        "patient_id":  pid,
                        "source":      envelope.get("event_type"),
                        "first_name":  data.get("first_name"),
                        "last_name":   data.get("last_name"),
                        "dob":         data.get("dob"),
                        "identity_sub":data.get("identity_sub"),
                    }
                    if existing:
                        db.execute(f"UPDATE {TABLE} SET data = data || %s, updated_at=now() WHERE id=%s",
                                   (Json(snapshot), existing["id"]))
                    else:
                        db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)", (Json(snapshot),))
        except Exception as e:
            log.exception("patient-search-service/patient.created handler failed: %s", e)
        emit_audit(bus, action="consume.patient.created", actor="system:patient-search-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.updated")
    def _on_patient_updated(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    pid = data.get("id") or data.get("patient_id")
                    if not pid: return
                    # Idempotent snapshot: keep the latest name/dob for this patient in our own store.
                    existing = db.query_one(
                        f"SELECT id FROM {TABLE} WHERE data->>'patient_id' = %s",
                        (str(pid),),
                    )
                    snapshot = {
                        "patient_id":  pid,
                        "source":      envelope.get("event_type"),
                        "first_name":  data.get("first_name"),
                        "last_name":   data.get("last_name"),
                        "dob":         data.get("dob"),
                        "identity_sub":data.get("identity_sub"),
                    }
                    if existing:
                        db.execute(f"UPDATE {TABLE} SET data = data || %s, updated_at=now() WHERE id=%s",
                                   (Json(snapshot), existing["id"]))
                    else:
                        db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)", (Json(snapshot),))
        except Exception as e:
            log.exception("patient-search-service/patient.updated handler failed: %s", e)
        emit_audit(bus, action="consume.patient.updated", actor="system:patient-search-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.merged")
    def _on_patient_merged(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    old_id = data.get("old_id") or data.get("from_id")
                    new_id = data.get("new_id") or data.get("to_id")
                    if not (old_id and new_id): return
                    n = db.execute(
                        f"UPDATE {TABLE} SET data = jsonb_set(data, '{{patient_id}}', to_jsonb(%s::text)), updated_at=now() "
                        f"WHERE data->>'patient_id' = %s",
                        (str(new_id), str(old_id)),
                    )
                    log.info("patient.merged: %d rows re-linked %s->%s", n, old_id, new_id)
        except Exception as e:
            log.exception("patient-search-service/patient.merged handler failed: %s", e)
        emit_audit(bus, action="consume.patient.merged", actor="system:patient-search-service",
                   target=None, details={"envelope_id": envelope.get("id")})

