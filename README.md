# Coupon Mention Tracker

## Overview

The **Coupon Mention Tracker** monitors coupon code mentions in Google AI Overviews across targeted keywords. It leverages existing AI monitoring infrastructure to detect when coupons appear in search results, sends weekly Slack reports, and enables historical correlation analysis with revenue data in Looker.

## Table of Contents

- [Overview](#overview)
- [Architecture & Logic](#architecture--logic)
- [Key Features](#key-features)
- [Prerequisites](#prerequisites)
- [Installation & Configuration](#installation--configuration)
- [Usage](#usage)
- [Development](#development)
- [Deployment](#deployment)
- [Troubleshooting & FAQ](#troubleshooting--faq)
- [Maintainers](#maintainers)

## Architecture & Logic

The system reads from the existing Marketing Hub AI Overviews database, scans response text for configured coupon codes, and generates reports.

### Data Flow Diagram

```mermaid
flowchart TD
    S1["1 Fetch AI Overview<br>Results from DB"]
    S2["2 Scan Response Text<br>for Coupon Patterns"]
    S3["3 Match Against<br>Active Coupon List"]
    S4["4 Generate Weekly<br>Report Data"]
    S5["5 Send Slack<br>Notification"]
    S6["6 Store Results<br>for Looker"]

    S1 --> S2
    S2 --> S3
    S3 --> S4
    S4 --> S5
    S4 --> S6

    classDef prepStyle fill:#7E7AE6,stroke:#3D3270,stroke-width:2px,color:#FFFFFF
    classDef analysisStyle fill:#3A5CED,stroke:#18407F,stroke-width:2px,color:#FFFFFF
    classDef decisionStyle fill:#85A2FF,stroke:#18407F,stroke-width:2px,color:#FFFFFF
    classDef validationStyle fill:#82E5E8,stroke:#1C8BA5,stroke-width:2px,color:#FFFFFF
    classDef documentationStyle fill:#C2A9FF,stroke:#3D3270,stroke-width:2px,color:#FFFFFF

    class S1 prepStyle
    class S2,S3 analysisStyle
    class S4 decisionStyle
    class S5,S6 documentationStyle
```

### Logical Flow

1. **Fetch Results**: Query `ai_overviews_results` joined with `ai_overviews_prompts` for the reporting period
2. **Scan Text**: Use regex patterns to find coupon codes in `response_text`
3. **Validate Coupons**: Check detected codes against the active coupon list
4. **Generate Report**: Aggregate by keyword, flag invalid/outdated coupons
5. **Notify**: Send formatted report to Slack channel
6. **Persist**: Data available in DB for Looker dashboards

## Key Features

- **Coupon Detection**: Scans AI Overview response text for configured coupon codes
- **Invalid Coupon Alerts**: Flags coupons not in the active list (potentially outdated)
- **Weekly Slack Reports**: Summarizes coupon appearances by keyword and location
- **Cloud Run Job**: Deployed as a scheduled Cloud Run Job

## Prerequisites

Before running locally, ensure you have:

- **Python**: Version 3.12 or higher
- **uv**: Python package manager
- **Database Access**: PostgreSQL credentials for Marketing Hub database
- **Slack Webhook**: Webhook URL for your alert channel

## Installation & Configuration

### 1. Clone and Install

```bash
git clone https://github.com/user/coupon-mention-tracker.git
cd coupon-mention-tracker

uv sync
```

### 2. Environment Configuration

Create a `.env` file in the project root:

| Variable                | Description                              | Required | Default          |
|:------------------------|:-----------------------------------------|:--------:|:-----------------|
| `DATABASE_URL`          | PostgreSQL connection string             |   Yes    | -                |
| `SLACK_WEBHOOK_URL`     | Webhook URL for sending alerts           |   Yes    | -                |
| `SLACK_CHANNEL`         | Default channel for notifications        |    No    | `#coupon-alerts` |
| `COUPONS`               | JSON array of coupon codes to track      |   Yes    | `[]`             |
| `REPORT_LOOKBACK_DAYS`  | Days to include in weekly report         |    No    | `7`              |

Example `.env`:

```bash
DATABASE_URL=postgresql://user:pass@host:5432/growth-marketing-db
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/xxx/xxx
COUPONS=["NORDVPN70","NORD70OFF","VPNDEAL2024"]
REPORT_LOOKBACK_DAYS=7
```

## Usage

### Running Locally

```bash
# Run as module
python -m coupon_mention_tracker

# Or directly
python src/coupon_mention_tracker/main.py
```

### Environment Variables Override

```bash
# Override lookback days
REPORT_LOOKBACK_DAYS=14 python -m coupon_mention_tracker
```

## Development

### Project Structure

```
coupon-mention-tracker/
├── src/
│   └── coupon_mention_tracker/
│       ├── __init__.py
│       ├── __main__.py                    # Package entry point
│       ├── main.py                        # Cloud Run Job entry point
│       ├── core/
│       │   ├── __init__.py
│       │   ├── config.py                  # Settings management
│       │   └── models/
│       │       ├── __init__.py
│       │       └── coupon_mention.py      # Data models
│       ├── repositories/
│       │   ├── __init__.py
│       │   └── ai_overview.py             # DB queries
│       ├── services/
│       │   ├── __init__.py
│       │   ├── coupon_matcher.py          # Pattern matching
│       │   └── report.py                  # Report generation
│       └── clients/
│           ├── __init__.py
│           └── slack.py                   # Slack API client
├── tests/
├── pyproject.toml
├── cloudbuild.yaml
├── Dockerfile
├── .env.example
└── README.md
```

### Quality Checks

```bash
# Format
uv run ruff format .

# Lint
uv run ruff check .

# Type check
uv run ty check

# Run tests
uv run pytest
```

## Deployment

### Google Cloud Run Job

The application is deployed as a Cloud Run Job, triggered weekly by Cloud Scheduler.

#### Prerequisites

```bash
# Create Artifact Registry repository
gcloud artifacts repositories create cloud-run-jobs \
  --repository-format=docker \
  --location=europe-west3

# Store secrets in Secret Manager
echo -n "https://hooks.slack.com/..." | \
  gcloud secrets create slack-webhook --data-file=-
```

#### Deploy with Cloud Build

The `cloudbuild.yaml` runs lint, type checks, builds the image, and deploys:

```bash
gcloud builds submit \
  --config=cloudbuild.yaml \
  --substitutions=_REGION=europe-west3
```

#### Manual Deploy (Alternative)

```bash
gcloud run jobs deploy coupon-mention-tracker \
  --source . \
  --region europe-west3 \
  --set-env-vars "DATABASE_URL=..." \
  --set-secrets "SLACK_WEBHOOK_URL=slack-webhook:latest"
```

#### Configure Environment Variables

```bash
gcloud run jobs update coupon-mention-tracker \
  --region europe-west3 \
  --set-env-vars "REPORT_LOOKBACK_DAYS=7" \
  --set-env-vars 'COUPONS=["NORDVPN70","NORD70OFF"]' \
  --set-secrets "DATABASE_URL=db-url:latest,SLACK_WEBHOOK_URL=slack-webhook:latest"
```

#### Schedule with Cloud Scheduler

```bash
gcloud scheduler jobs create http coupon-tracker-weekly \
  --location europe-west3 \
  --schedule "0 9 * * 1" \
  --uri "https://europe-west3-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT/jobs/coupon-mention-tracker:run" \
  --http-method POST \
  --oauth-service-account-email SERVICE_ACCOUNT@PROJECT.iam.gserviceaccount.com
```

## Troubleshooting & FAQ

### Common Issues

**1. Database Connection Failed**
- **Cause**: Invalid credentials or network access
- **Resolution**: Verify `DATABASE_URL` and VPC connector settings

**2. No Coupons Detected**
- **Cause**: Coupons list is empty or patterns don't match
- **Resolution**: Check `COUPONS` env var is a valid JSON array

**3. Slack Message Not Sent**
- **Cause**: Invalid webhook URL or network issues
- **Resolution**: Verify `SLACK_WEBHOOK_URL` in Secret Manager

### FAQ

**How often does the job run?**
Weekly (Monday 9 AM) via Cloud Scheduler.

**Can I track coupons for multiple products?**
Yes, the system tracks by product automatically based on keyword associations.

**What happens if an outdated coupon is detected?**
It will be flagged in logs and highlighted in the Slack notification.

## Maintainers

- **Growth Marketing Department**
- **Primary Contact**: Vytautas Bunevičius (vytautas.bunevicius@nordsec.com)
