# Security Policy

## Supported Versions

This is a portfolio/learning project. Only the latest code on `main` is actively maintained.

| Branch | Supported |
|--------|-----------|
| main   | Yes       |

## Reporting a Vulnerability

If you discover a security vulnerability, please **do not** open a public GitHub issue.

Instead, open a [GitHub Security Advisory](https://github.com/egrynspan/data-engineering-portfolio/security/advisories/new) (private disclosure).

I aim to respond within 5 business days and will work to address confirmed vulnerabilities promptly.

## Security Practices

- Dependencies are pinned to version ranges and scanned weekly via Dependabot
- Static analysis (bandit) and dependency CVE scanning (pip-audit) run on every CI build
- AWS credentials are never committed — all secrets are injected via environment variables or AWS IAM roles
- No production data is stored in this repository
