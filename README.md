# FraudCore

Real-time fraud detection pipeline using Python, Kafka, Spark Structured Streaming, and Streamlit.

## Fraud Detection Rules

| # | Rule                  | Logic                                                       |
|---|-----------------------|-------------------------------------------------------------|
| 1 | HIGH_VALUE            | Single transaction amount > 1 000                           |
| 2 | RAPID_ACTIVITY        | Same user performs > 3 transactions within 1 minute         |
| 3 | GEO_ANOMALY           | Same user appears in > 1 location within 1 minute           |
| 4 | REPEATED_HIGH_VALUE   | Same user makes ≥ 3 transactions > 1 000 within 2 minutes   |
| 5 | CRYPTO_RISK           | Payment method = crypto AND amount > 500                    |
| 6 | VELOCITY_SPIKE        | Same user spends > 5 000 total within 1 minute              |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+

