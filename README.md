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

Since around late September 2026 the V3 endpoint returns some numeric fields as strings (for example `out_pac` as `"0"` and inverter `status` as `"-1"`). On `py3.13.1` and earlier this shows up as an `Unknown` status in the log followed by `type str doesn't define __round__ method`. From `py3.13.2-beta1` all numeric GoodWe fields are parsed to numbers before use.

The SEMS+ login may also return a gateway API host such as `https://au-gateway.semsportal.com/web/sems`. That gateway serves the plant API under `sems-plant/api/`, so calling `v3/...` directly on it fails with `404 Not Found`. From `py3.13.2-beta2` the client tries the gateway with the `sems-plant/api/` prefix first, then falls back to the legacy regional host (`https://<region>.semsportal.com/api/`). It no longer retries 404s, so a failure is reported before the Lambda timeout is reached.

Since 23 September 2026 the legacy V3 monitor endpoint returns success but no inverter data (all readings `0`, status `Unknown`). From `py3.13.2-beta3` the readings come from the SEMS+ Web plant API on the regional gateway, the same API the SEMS+ web UI and the [goodwe-sems-home-assistant](https://github.com/TimSoethout/goodwe-sems-home-assistant) integration use:

- `GET sems-plant/api/stations/device/all-status`: inverters and their status
- `GET sems-plant/api/equipments/{sn}/telemetry`: AC power, grid voltage, temperature, PV voltages
- `GET sems-plant/api/equipments/{sn}/telecounting`: energy today and lifetime per inverter
- `GET sems-plant/api/stations/flow`: station power flow, used for load
- `POST sems-plant/api/stations/statistics`: today's generation and consumption

If the SEMS+ Web calls fail, the client falls back to the legacy endpoint.

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

Versions containing `alpha`, `beta` or `rc` (for example `py3.13.2-beta1`) are published as GitHub prereleases. To cut a release, bump `VERSION` and push to `main`.
