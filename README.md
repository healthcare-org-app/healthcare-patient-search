# patient-search-service

patient-search-service — domain: patients

- **Port:** 8107
- **Language:** Python 3.11 + Flask
- **Database:** `patients` (Postgres, table `patient_search`)
- **Event bus:** Kafka

## API

| Method    | Path                       |
|-----------|----------------------------|
| GET       | `/api/patient_search/`          |
| POST      | `/api/patient_search/`          |
| GET       | `/api/patient_search/<id>`      |
| PUT/PATCH | `/api/patient_search/<id>`      |
| DELETE    | `/api/patient_search/<id>`      |
| GET       | `/health`                  |
| GET       | `/ready`                   |

## Events

**Publishes:** (none)
**Subscribes:** patient.created, patient.updated, patient.merged

## HTTP peer dependencies

- `patients-service`
- `audit-log-service`

## Local dev

```bash
pip install -e ../../libs/py-healthcare-common
pip install -r requirements.txt
cp .env.example .env
(cd ../../infra && docker compose up -d postgres kafka kafka-init)
python -m app.main
```

## Tests

```bash
pytest
```
