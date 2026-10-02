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
        self.session = None

    @staticmethod
    def statusText(status):
        labels = {-1: "Offline", 0: "Waiting", 1: "Normal", 2: "Fault"}
        return labels.get(status, "Unknown")

    @staticmethod
    def parseValue(value, unit):
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).rstrip(unit))
        except (TypeError, ValueError) as exp:
            logging.warning(exp)
            return 0

    def calcPvVoltage(self, data):
        pv_voltages = []
        for i in range(1, 5):
            key = "vpv" + str(i)
            if key not in data:
                continue
            value = data[key]
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

        if api:
            self.base_url = str(api).rstrip("/") + "/"
        elif region:
            self.base_url = "https://{}.semsportal.com/api/".format(region)
        else:
            self.base_url = self.FALLBACK_API

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
        self.session["api"] = self.base_url.rstrip("/")

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

    def call(self, url, payload):
        last_error = None

        for attempt in range(1, 4):
            try:
                if self.session is None:
                    self._login()

                response = requests.post(
                    self.base_url + url,
                    headers=self._headers(),
                    data=payload,
                    timeout=10,
                )
                response.raise_for_status()
                body = response.json()
                logging.debug(body)

                if self._success(body.get("code")) and body.get("data") is not None:
                    return body["data"]

                code = body.get("code")
                message = (
                    body.get("description")
                    or body.get("msg")
                    or body.get("message")
                    or "unknown GoodWe API error"
                )

                if code in (100001, 100002, "100001", "100002") or "token" in str(message).lower():
                    self.session = None
                    continue

                raise RuntimeError(
                    "Failed to call GoodWe API (code {}): {}".format(code, message)
                )

            except (requests.exceptions.RequestException, ValueError, RuntimeError) as exp:
                last_error = exp
                logging.warning(exp)

            time.sleep(attempt ** 3)

        raise RuntimeError(
            "Failed to call GoodWe API after retries: {}".format(last_error)
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
            result["eday_kwh"] = float(stats.get("sum", 0) or 0)
            result["consumptionOfLoad"] = float(
                stats.get("consumptionOfLoad", 0) or 0
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
                result["pgrid_w"] += inverter_data.get("out_pac", 0) or 0
                result["grid_voltage"] += self.parseValue(
                    inverter_data.get("output_voltage", 0), "V"
                )
                result["itemp"] += inverter_data.get("tempperature", 0) or 0
                result["pv_voltage"] += self.calcPvVoltage(inverter_data.get("d") or {})
                count += 1

            if not has_statistics:
                result["eday_kwh"] += inverter_data.get("eday", 0) or 0

            result["etotal_kwh"] += inverter_data.get("etotal", 0) or 0

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
                result["pgrid_w"] = inverter_data.get("out_pac", 0) or 0

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
            pgrid_w = sample["pac"]

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
