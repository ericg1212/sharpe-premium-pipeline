# Security Policy

## Reporting a Vulnerability

To report a security vulnerability, please open a GitHub issue with the label `security`.

## Data Handling

- All financial data is sourced from public APIs (SEC EDGAR, FRED, Yahoo Finance) — no proprietary or non-public information is stored or processed
- `.env` files containing API credentials are excluded from version control via `.gitignore`
- AWS credentials follow least-privilege IAM principles

## Secrets Management

- API keys and credentials are managed via environment variables only
- See `.env.example` for required variables — never commit a populated `.env` file
- AWS access is scoped to the minimum permissions required (S3, Glue, Athena read/write)
