import base64
import hashlib
import json
import logging
import time
from datetime import datetime

import requests


class GoodWeApi:
    """GoodWe SEMS client using the SEMS+ cross-login flow.

    The public interface is intentionally kept compatible with the original
    gw2pvo code. Credentials are supplied by the Lambda event at runtime and
    are never stored in this module.
    """

    LOGIN_URL = "https://eu-semsplus.goodwe.com/web/sems/sems-user/api/v1/auth/cross-login"
    FALLBACK_API = "https://www.semsportal.com/api/"
    CLIENT = "semsPlusWeb"

    def __init__(self, system_id, account, password):
        self.system_id = system_id
        self.account = account
        self.password = password
        self.base_url = self.FALLBACK_API
        self.api_bases = [self.FALLBACK_API]
        self.session = None

    @staticmethod
    def statusText(status):
        labels = {-1: "Offline", 0: "Waiting", 1: "Normal", 2: "Fault"}
        # The V3 API may return the status as a string, e.g. "-1".
        try:
            status = int(status)
        except (TypeError, ValueError):
            pass
        return labels.get(status, "Unknown")

    @staticmethod
    def parseValue(value, unit):
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return float(value)
        try:
            text = str(value).strip()
            if text.endswith(unit.strip()):
                text = text[: len(text) - len(unit.strip())]
            return float(text.strip())
        except (TypeError, ValueError) as exp:
            logging.warning(exp)
            return 0

    def calcPvVoltage(self, data):
        pv_voltages = []
        for i in range(1, 5):
            key = "vpv" + str(i)
            if key not in data:
                continue
            value = self.parseValue(data[key], "V")
            if value and value < 6553:
                pv_voltages.append(value)
        return round(sum(pv_voltages), 1)

    @staticmethod
    def _success(code):
        return code in (0, "0", "00000")

    @staticmethod
    def _signature(uid, token):
        timestamp = int(time.time() * 1000)
        digest = hashlib.sha256(
            "{}@{}@{}".format(timestamp, uid, token).encode("utf-8")
        ).hexdigest()
        raw = "{}@{}".format(digest, timestamp).encode("utf-8")
        return base64.b64encode(raw).decode("ascii")

    def _login(self):
        # SEMS+ expects base64(md5(password).hexdigest()), not the cleartext
        # password used by the legacy CrossLogin endpoint.
        password_md5 = hashlib.md5(self.password.encode("utf-8")).hexdigest()
        encoded_password = base64.b64encode(password_md5.encode("ascii")).decode("ascii")

        token_header = {
            "uid": "",
            "timestamp": 0,
            "token": "",
            "client": self.CLIENT,
            "version": "",
            "language": "en",
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://eu-semsplus.goodwe.com",
            "Referer": "https://eu-semsplus.goodwe.com/",
            "token": json.dumps(token_header),
            "x-signature": self._signature("", ""),
        }

        payload = {
            "account": self.account,
            "pwd": encoded_password,
            "agreement": 1,
            "isChinese": False,
            "isLocal": False,
        }

        response = requests.post(
            self.LOGIN_URL, headers=headers, json=payload, timeout=10
        )
        response.raise_for_status()
        body = response.json()

        if not self._success(body.get("code")):
            message = (
                body.get("description")
                or body.get("msg")
                or body.get("message")
                or "unknown SEMS+ login error"
            )
            raise RuntimeError("GoodWe SEMS+ login failed: {}".format(message))

        data = body.get("data") or {}
        uid = str(data.get("uid") or "")
        token = str(data.get("token") or "")
        if not uid or not token:
            raise RuntimeError("GoodWe SEMS+ login returned no usable session")

        api = body.get("api") or data.get("api")
        region = body.get("region") or data.get("region")

        # Since 2026 the login may return a SEMS+ gateway such as
        # https://au-gateway.semsportal.com/web/sems, which serves the plant
        # API under sems-plant/api/. The legacy regional host is kept as a
        # fallback in case the gateway route is not available.
        self.api_bases = []
        if api:
            api = str(api).rstrip("/")
            if "gateway" in api:
                self.api_bases.append(api + "/sems-plant/api/")
            else:
                self.api_bases.append(api + "/")
        if region:
            self.api_bases.append("https://{}.semsportal.com/api/".format(region))
        self.api_bases.append(self.FALLBACK_API)
        self.base_url = self.api_bases[0]
        logging.info("SEMS+ login: api=%s region=%s hosts=%s", api, region, self.api_bases)

        self.session = {
            "uid": uid,
            "timestamp": str(data.get("timestamp") or int(time.time() * 1000)),
            "token": token,
            "client": self.CLIENT,
            "version": "",
            "language": "en",
        }
        if region:
            self.session["region"] = str(region)
        self.session["api"] = api or self.base_url.rstrip("/")

    def _headers(self):
        if self.session is None:
            self._login()

        return {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "PVMaster/2.9.5 (iPhone; iOS 17.5; Scale/3.00)",
            "token": json.dumps(self.session),
            "x-signature": self._signature(
                self.session["uid"], self.session["token"]
            ),
        }

    class _RouteNotFound(Exception):
        pass

    def _post(self, base_url, url, payload):
        response = requests.post(
            base_url + url,
            headers=self._headers(),
            data=payload,
            timeout=10,
        )
        if response.status_code == 404:
            raise self._RouteNotFound("404 Not Found for url: {}".format(response.url))
        response.raise_for_status()
        body = response.json()
        logging.debug(body)

        # The SEMS+ gateway answers unknown routes with HTTP 200.
        if "Route Not Found" in str(body.get("error_msg", "")):
            raise self._RouteNotFound(
                "{} for url: {}".format(body["error_msg"], response.url)
            )
        return body

    def call(self, url, payload):
        last_error = None

        if self.session is None:
            self._login()

        for base_url in list(self.api_bases):
            for attempt in range(1, 3):
                try:
                    if self.session is None:
                        self._login()

                    body = self._post(base_url, url, payload)

                    if self._success(body.get("code")) and body.get("data") is not None:
                        # Remember the working host for subsequent calls.
                        if base_url in self.api_bases:
                            self.api_bases.remove(base_url)
                        self.api_bases.insert(0, base_url)
                        self.base_url = base_url
                        return body["data"]

                    code = body.get("code")
                    message = (
                        body.get("description")
                        or body.get("msg")
                        or body.get("message")
                        or body.get("errorMsg")
                        or "unknown GoodWe API error"
                    )
                    last_error = RuntimeError(
                        "GoodWe API error at {} (code {}): {}".format(
                            base_url, code, message
                        )
                    )
                    logging.warning(last_error)

                    auth_error = (
                        code in (100001, 100002, "100001", "100002", "C0602")
                        or "token" in str(message).lower()
                    )
                    if auth_error and attempt == 1:
                        self.session = None
                        continue
                    break

                except self._RouteNotFound as exp:
                    last_error = exp
                    logging.warning(exp)
                    break

                except (requests.exceptions.RequestException, ValueError) as exp:
                    last_error = exp
                    logging.warning(exp)
                    if attempt == 1:
                        time.sleep(2)

        raise RuntimeError(
            "Failed to call GoodWe API on all hosts: {}".format(last_error)
        )

    def getCurrentReadings(self):
        """Download the most recent readings from the GoodWe API."""

        data = self.call(
            "v3/PowerStation/GetMonitorDetailByPowerstationId",
            {"powerStationId": self.system_id},
        )

        has_powerflow = data.get("hasPowerflow", False)
        has_statistics = data.get("hasEnergeStatisticsCharts", False)

        result = {
            "status": "Unknown",
            "itemp": 0,
            "pgrid_w": 0,
            "etotal_kwh": 0,
            "grid_voltage": 0,
            "pv_voltage": 0,
            "latitude": data.get("info", {}).get("latitude"),
            "longitude": data.get("info", {}).get("longitude"),
            "eday_kwh": 0,
            "consumptionOfLoad": None,
            "load": None,
        }

        if has_statistics:
            stats = data.get("energeStatisticsCharts") or {}
            result["eday_kwh"] = self.parseValue(stats.get("sum"), "kWh")
            result["consumptionOfLoad"] = self.parseValue(
                stats.get("consumptionOfLoad"), "kWh"
            )

        if has_powerflow:
            powerflow = data.get("powerflow") or {}
            result["load"] = float(self.parseValue(powerflow.get("load", 0), " (W) "))

        count = 0
        inverters = data.get("inverter") or []

        for inverter_data in inverters:
            status = self.statusText(inverter_data.get("status"))
            if status == "Normal":
                result["status"] = status
                result["pgrid_w"] += self.parseValue(inverter_data.get("out_pac"), "W")
                result["grid_voltage"] += self.parseValue(
                    inverter_data.get("output_voltage", 0), "V"
                )
                result["itemp"] += self.parseValue(inverter_data.get("tempperature"), "C")
                result["pv_voltage"] += self.calcPvVoltage(inverter_data.get("d") or {})
                count += 1

            if not has_statistics:
                result["eday_kwh"] += self.parseValue(inverter_data.get("eday"), "kWh")

            result["etotal_kwh"] += self.parseValue(inverter_data.get("etotal"), "kWh")

        if count > 0:
            result["grid_voltage"] /= count
            result["pv_voltage"] /= count
            result["itemp"] /= count
        elif inverters:
            inverter_data = inverters[0]
            result["status"] = self.statusText(inverter_data.get("status"))

            if has_powerflow:
                result["pgrid_w"] = self.parseValue(
                    (data.get("powerflow") or {}).get("pv", 0), "(W)"
                )
            else:
                result["pgrid_w"] = self.parseValue(inverter_data.get("out_pac"), "W")

            result["grid_voltage"] = self.parseValue(
                inverter_data.get("output_voltage", 0), "V"
            )
            result["pv_voltage"] = self.calcPvVoltage(inverter_data.get("d") or {})

        message = (
            "{status}, {pgrid_w} W now, Load {load} W now, "
            "{eday_kwh} kWh today, {etotal_kwh} kWh all time, "
            "{consumptionOfLoad} kWh used today {grid_voltage} V grid, "
            "{pv_voltage} V PV, {itemp} C"
        ).format(**result)

        if result["status"] in ("Normal", "Offline"):
            logging.info(message)
        else:
            logging.warning(message)

        return result

    def getLocation(self):
        data = self.call(
            "v3/PowerStation/GetMonitorDetailByPowerstationId",
            {"powerStationId": self.system_id},
        )
        info = data.get("info") or {}
        return {
            "latitude": info.get("latitude"),
            "longitude": info.get("longitude"),
        }

    def getActualKwh(self, date):
        payload = {
            "powerstation_id": self.system_id,
            "count": 1,
            "date": date.strftime("%Y-%m-%d"),
        }
        data = self.call(
            "v2/PowerStationMonitor/GetPowerStationPowerAndIncomeByDay", payload
        )
        if not data:
            logging.warning("GetPowerStationPowerAndIncomeByDay missing data")
            return 0

        eday_kwh = 0
        for day in data:
            if day["d"] == date.strftime("%m/%d/%Y"):
                eday_kwh = day["p"]
        return eday_kwh

    def getDayPac(self, date):
        payload = {
            "id": self.system_id,
            "date": date.strftime("%Y-%m-%d"),
        }
        data = self.call(
            "v2/PowerStationMonitor/GetPowerStationPacByDayForApp", payload
        )
        if "pacs" not in data:
            logging.warning("GetPowerStationPacByDayForApp returned bad data: %s", data)
            return []
        return data["pacs"]

    def getDayReadings(self, date):
        result = self.getLocation()
        pacs = self.getDayPac(date)

        hours = 0
        kwh = 0
        result["entries"] = []

        for sample in pacs:
            parsed_date = datetime.strptime(sample["date"], "%m/%d/%Y %H:%M:%S")
            next_hours = parsed_date.hour + parsed_date.minute / 60
            pgrid_w = self.parseValue(sample.get("pac"), "W")

            if pgrid_w > 0:
                kwh += pgrid_w / 1000 * (next_hours - hours)
                result["entries"].append(
                    {
                        "dt": parsed_date,
                        "pgrid_w": pgrid_w,
                        "eday_kwh": round(kwh, 3),
                    }
                )

            hours = next_hours

        eday_kwh = self.getActualKwh(date)
        if eday_kwh > 0 and kwh > 0:
            correction = eday_kwh / kwh
            for sample in result["entries"]:
                sample["eday_kwh"] *= correction

        return result
