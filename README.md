# AWS Lambda GoodWe SEMS to PVOutput uploader

This project provides a lightweight, fully cloud-hosted bridge for sending GoodWe inverter data to PVOutput.

It runs as an AWS Lambda function, so there is no server, Raspberry Pi, Home Assistant instance or other always-on device required at home. Once deployed, AWS takes care of running it on schedule and the normal workload fits comfortably within the Lambda free tier, making the service effectively free to operate.

The uploader retrieves production data from GoodWe's cloud service and publishes it to PVOutput using the normal PVOutput status interface. This turned out to be particularly useful after changes to the older GoodWe/PVOutput integration path left a number of existing setups without a reliable way of continuing uploads. Rather than depending on a vendor appliance or a local polling service, the entire data path remains in the cloud and under the user's control.

The project is deliberately small and transparent. Configuration is handled through Lambda environment variables, there are no external servers to maintain, and the function can be inspected, modified or extended easily. It is intended for people who want their GoodWe data in PVOutput but would rather not dedicate local infrastructure just to keep a simple telemetry feed alive.

## GoodWe API status

This project no longer relies on the old GoodWe API path that many older integrations used and that eventually stopped working.

The current implementation (`py3.13.2`) uses the same SEMS+ stack as GoodWe's current web UI:

- SEMS+ cross-login authentication (`semsPlusWeb` client, signed token headers)
- the regional SEMS+ gateway returned by login, e.g. `https://au-gateway.semsportal.com/web/sems`
- the SEMS+ Web plant API on that gateway:
  - `GET sems-plant/api/stations/device/all-status`: inverters and their status
  - `GET sems-plant/api/equipments/{sn}/telemetry`: AC power, grid voltage, temperature, PV voltages
  - `GET sems-plant/api/equipments/{sn}/telecounting`: energy today and lifetime per inverter
  - `GET sems-plant/api/stations/flow`: station power and load
  - `POST sems-plant/api/stations/statistics`: today's generation and consumption

The endpoints and value codes follow the working [goodwe-sems-home-assistant](https://github.com/TimSoethout/goodwe-sems-home-assistant) integration.

On 23 September 2026 GoodWe stopped returning inverter data from the legacy `v3/PowerStation/GetMonitorDetailByPowerstationId` endpoint. It still answers with success, but the data is empty. With `py3.13.1` and earlier this shows up as `Unknown, 0 W now` in the log, `type str doesn't define __round__ method` on upload, or a `404 Not Found` from the gateway followed by a Lambda timeout. Upgrade to `py3.13.2` to fix it. The legacy endpoint is kept only as a fallback.

The public Python interface was deliberately kept compatible with the original code: `GoodWeApi(...).getCurrentReadings()` still returns the data expected by the Lambda, so the migration did not require rewriting the surrounding PVOutput logic.

"Today" totals use the Lambda's local date. Set the `TZ` environment variable (for example `Australia/Sydney`) to match the plant.

## Background

The project was initially inspired by Mark Ruys' `gw2pvo` project and evolved from my fork, with later changes for AWS Lambda and newer GoodWe SEMS access.

## Python 3.13

The current Lambda runtime target is Python 3.13.

The release layer is rebuilt from source with `requirements-layer.txt`; the old committed layer ZIP is legacy only.

Dark Sky support was removed from the current Lambda path.

Credentials are supplied at runtime by the Lambda event and are not stored in this repository.

## Releases

Releases are built by GitHub Actions (`.github/workflows/release.yml`) on every push to `main` that touches the Lambda code, the layer requirements or `VERSION`. The release tag is taken from the `VERSION` file, and each release carries two assets:

- `gw2pvo-layer-<version>.zip` – Lambda layer with `gw2pvo` and its dependencies
- `GW2PVO-<version>.zip` – the Lambda function code

Versions containing `alpha`, `beta` or `rc` (for example `py3.13.2-beta1`) are published as GitHub prereleases. The release title and description come from `RELEASE_NOTES.md`: its first `## ` heading becomes the title and the rest the description. To cut a release, update `RELEASE_NOTES.md`, bump `VERSION` and push to `main`.
