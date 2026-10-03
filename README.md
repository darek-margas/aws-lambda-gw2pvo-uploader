# AWS Lambda GoodWe SEMS to PVOutput uploader

This project provides a lightweight, fully cloud-hosted bridge for sending GoodWe inverter data to PVOutput.

It runs as an AWS Lambda function, so there is no server, Raspberry Pi, Home Assistant instance or other always-on device required at home. Once deployed, AWS takes care of running it on schedule and the normal workload fits comfortably within the Lambda free tier, making the service effectively free to operate.

The uploader retrieves production data from GoodWe's cloud service and publishes it to PVOutput using the normal PVOutput status interface. This turned out to be particularly useful after changes to the older GoodWe/PVOutput integration path left a number of existing setups without a reliable way of continuing uploads. Rather than depending on a vendor appliance or a local polling service, the entire data path remains in the cloud and under the user's control.

The project is deliberately small and transparent. Configuration is handled through Lambda environment variables, there are no external servers to maintain, and the function can be inspected, modified or extended easily. It is intended for people who want their GoodWe data in PVOutput but would rather not dedicate local infrastructure just to keep a simple telemetry feed alive.

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
