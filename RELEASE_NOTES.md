## py3.13.2: SEMS+ Web plant API

Restores PVOutput uploads after GoodWe's SEMS change of 23 September 2026.

### What broke

Since 23 September 2026 the legacy `v3/PowerStation/GetMonitorDetailByPowerstationId` endpoint returns success with no inverter data. With `py3.13.1` and earlier this shows up as:

- `Unknown, 0 W now, ... 0 kWh today` in the log, and
- `type str doesn't define __round__ method` on upload, or
- `404 Not Found` for `https://<region>-gateway.semsportal.com/web/sems/v3/...` followed by a Lambda timeout.

### What changed

- Readings now come from the SEMS+ Web plant API on the regional gateway returned by login (for example `https://au-gateway.semsportal.com/web/sems`). This is the API the SEMS+ web UI and the [goodwe-sems-home-assistant](https://github.com/TimSoethout/goodwe-sems-home-assistant) integration use:
  - `stations/device/all-status`: inverters and their status
  - `equipments/{sn}/telemetry`: AC power, grid voltage, temperature, PV voltages
  - `equipments/{sn}/telecounting`: energy today and lifetime
  - `stations/flow`: station power and load
  - `stations/statistics`: today's generation and consumption
- Numeric values returned as strings are converted to numbers before use.
- SEMS+ text statuses are recognised. An inverter that is producing power is treated as `Normal`.
- The legacy endpoint is kept as a fallback.
- The 404 retries and the long backoff are gone, so failures are reported well inside the Lambda timeout.
- `getCurrentReadings()` returns the same shape as before, so `lambda_function.py` is unchanged.

### Upgrade

1. Publish a new layer version from `gw2pvo-layer-py3.13.2.zip` and attach it to the function.
2. The function code (`GW2PVO-py3.13.2.zip`) and the event format are unchanged.
3. Optional: set the `TZ` environment variable (for example `Australia/Sydney`) so "today" totals use the plant's local day.
