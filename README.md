# AWS Lambda GoodWe SEMS to PVOutput uploader

Send GoodWe inverter data from SEMS directly to PVOutput with a tiny scheduled AWS Lambda.

This is a fully cloud-to-cloud setup: nothing has to run at home. No Raspberry Pi, NAS, Home Assistant, Docker container or always-on server is required. AWS Lambda wakes up on schedule, reads the current GoodWe data, uploads it to PVOutput, and stops again. At this workload it is effectively free in normal low-volume use.

The main reason for keeping this as a small standalone Lambda is control. You own the code, the upload interval and the mapping into PVOutput, without depending on a permanently running local service.

## GoodWe API status

This project no longer relies on the old GoodWe API path that many older integrations used and that eventually stopped working.

The current implementation uses the same newer SEMS+ login flow used by GoodWe's current web stack:

- SEMS+ cross-login authentication
- region returned by the login response
- regional SEMS API host
- V3 `PowerStation/GetMonitorDetailByPowerstationId` monitor endpoint
- the same authenticated token/signature style expected by the current service

That means this is more than just changing an endpoint name or bumping an API version. The authentication flow and API routing were updated together, which is why it continues to work after the legacy GoodWe endpoint was retired.

The public Python interface was deliberately kept compatible with the original code: `GoodWeApi(...).getCurrentReadings()` still returns the data expected by the Lambda, so the migration did not require rewriting the surrounding PVOutput logic.

## Background

The project was initially inspired by Mark Ruys' `gw2pvo` project and evolved from my fork, with later changes for AWS Lambda and newer GoodWe SEMS access.

## Python 3.13

The current Lambda runtime target is Python 3.13.

The release layer is rebuilt from source with `requirements-layer.txt`; the old committed layer ZIP is legacy only.

Dark Sky support was removed from the current Lambda path.

Credentials are supplied at runtime by the Lambda event and are not stored in this repository.
