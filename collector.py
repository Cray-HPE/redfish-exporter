from prometheus_client.core import GaugeMetricFamily
import requests
import logging
import os
import time
import sys
import re
from collectors.health_collector import HealthCollector

class RedfishMetricsCollector(object):

    def __enter__(self):
        return self

    def __init__(self, config, target, host, rf_port, usr, pwd, metrics_type):
        self.target = target
        self.host = host
        self.rf_port = rf_port
        self._username = usr
        self._password = pwd
        
        self.metrics_type = metrics_type

        self._timeout = int(os.getenv("TIMEOUT", config.get('timeout', 10)))
        self.labels = {"host": self.host,"redfish_instance": f"{self.target}:9220"}
        self._redfish_up = 0
        self._response_time = 0
        self._last_http_code = 0
        self.powerstate = 0

        self.urls = {
            "Systems": "",
            "StorageServices": "",
            "SessionService": "",
            "CapacitySources": "",
            "ProvidingDrives": ""
        }

        self.server_health = 0

        self.manufacturer = ""
        self.model = ""
        self.serial = ""
        self.status = {
            "ok": 1,
            "enabled": 0,
            "critical": 0,
            "error": 0,
            "warning": 0,
            "absent": 0,
            "unknown": 0,
            "not available": 0,
            "non-critical": 0,
            "not installed": 0,
            "unrecoverable": 0,
            "unsupported": 0
        }
        self._start_time = time.time()

        self._session_url = ""
        self._auth_token = ""
        self._basic_auth = False
        self._session = ""

    def get_session(self):
        # Get the url for the server info and messure the response time
        logging.info("Target %s: Connecting to server %s", self.target, self.host)
        logging.debug("Target %s: Attempting initial connection to /redfish/v1", self.target)
        start_time = time.time()
        server_response = self.connect_server("/redfish/v1", noauth=True)
        self._response_time = round(time.time() - start_time, 2)
        logging.info("Target %s: Response time: %s seconds.", self.target, self._response_time)

        if not server_response:
            logging.warning("Target %s: No data received from server %s!", self.target, self.host)
            return

        logging.debug("Target %s: data received from server %s.", self.target, self.host)
     
        for key in ["SessionService", "StorageServices"]:
            if key in server_response:
                self.urls[key] = server_response[key]['@odata.id']
                logging.debug("Target %s: Found %s URL: %s", self.target, key, self.urls[key])
            else:
                logging.warning(
                    "Target %s: No %s URL found on server %s!",
                    self.target,
                    key,
                    self.host
                )
                return
                
        session_service = self.connect_server(
            "/redfish/v1", 
            basic_auth=True
        )
        
        logging.debug("Target %s: Session service response status: %s", self.target, self._last_http_code)
         
        if self._last_http_code != 200:
            logging.warning(
                "Target %s: Failed to get a session from server %s!",
                self.target,
                self.host
            )
            self._basic_auth = True
            return

        sessions_url = f"https://{self.target}:{self.rf_port}{session_service['SessionService']['@odata.id']}/Sessions"
        logging.debug("Target %s: Attempting session creation at: %s", self.target, sessions_url)
        session_data = {"UserName": self._username, "Password": self._password}
        self._session.auth = None
        result = ""

        # Try to get a session
        try:
            result = self._session.post(
                sessions_url, json=session_data, verify=False, timeout=self._timeout
            )
            result.raise_for_status()

        except requests.exceptions.ConnectionError:
            logging.warning(
                "Target %s: Failed to get an auth token from server %s. Retrying ...",
                self.target, self.host
            )
            try:
                result = self._session.post(
                    sessions_url, json=session_data, verify=False, timeout=self._timeout
                )
                result.raise_for_status()

            except requests.exceptions.ConnectionError as e:
                logging.error(
                    "Target %s: Error getting an auth token from server %s: %s",
                    self.target, self.host, e
                )
                self._basic_auth = True

        except requests.exceptions.HTTPError as err:
            logging.warning(
                "Target %s: No session received from server %s: %s",
                self.target, self.host, err
            )
            logging.warning("Target %s: Switching to basic authentication.",
                    self.target
            )
            self._basic_auth = True

        except requests.exceptions.ReadTimeout as err:
            logging.warning(
                "Target %s: No session received from server %s: %s",
                self.target, self.host, err
            )
            logging.warning("Target %s: Switching to basic authentication.",
                    self.target
            )
            self._basic_auth = True

        if result:
            if result.status_code in [200, 201]:
                self._auth_token = result.headers['X-Auth-Token']
                self._session_url = result.json()['@odata.id']
                logging.info("Target %s: Got an auth token from server %s!", self.target, self.host)
                logging.debug("Target %s: Session URL: %s", self.target, self._session_url)
                self._redfish_up = 1

    def connect_server(self, command, noauth=False, basic_auth=False):
        logging.captureWarnings(True)

        req = ""
        req_text = ""
        server_response = ""
        self._last_http_code = 200
        request_duration = 0
        request_start = time.time()
        base_url = f"https://{self.target}:{self.rf_port}"
        url = f"{base_url}{command}"
        command=""

        # check if we already established a session with the server
        if not self._session:
            self._session = requests.Session()
            logging.debug("Target %s: Created new session", self.target)
        else:
            logging.debug("Target %s: Using existing session.", self.target)

        self._session.verify = False
        self._session.headers.update({"charset": "utf-8"})
        self._session.headers.update({"content-type": "application/json"})
        self._session.headers.update({"k": "true"})

        if noauth:
            logging.debug("Target %s: Using no auth", self.target)
        elif basic_auth or self._basic_auth:
            self._session.auth = (self._username, self._password)
            logging.debug(f"Target {self.target}: Using basic auth with user {self._username}")
        else:
            logging.debug("Target %s: Using auth token", self.target)
            self._session.auth = None
            self._session.headers.update({"X-Auth-Token": self._auth_token})

        logging.info("Target %s: Using URL %s", self.target, url)
        logging.debug("Target %s: Request headers: %s", self.target, dict(self._session.headers))
        try:
            req = self._session.get(url)
            req.raise_for_status()
            logging.debug("Target %s: Request successful, status: %s", self.target, req.status_code)

        except requests.exceptions.HTTPError as err:
            self._last_http_code = err.response.status_code

            if err.response.status_code == 401:
                logging.error(
                    "Target %s: Authorization Error: "
                    "Wrong user/password set wrong on server %s: %s",
                    self.target, self.host, err
                )
            elif not req.status_code in [200, 201]:
               return req.status_code

        except requests.exceptions.ConnectTimeout:
            logging.error("Target %s: Timeout while connecting to %s", self.target, self.host)
            self._last_http_code = 408

        except requests.exceptions.ReadTimeout:
            logging.error("Target %s: Timeout while reading data from %s", self.target, self.host)
            self._last_http_code = 408

        except requests.exceptions.ConnectionError as err:
            logging.error("Target %s: Unable to connect to %s: %s", self.target, self.host, err)
            self._last_http_code = 444
        
        if req != "":
            self._last_http_code = req.status_code
            try:
                req_text = req.json()

            except requests.JSONDecodeError:
                logging.info("Target %s: No json data received.", self.target)

            # req will evaluate to True if the status code was between 200 and 400 and False otherwise.
            if req:
                server_response = req_text

            # if the request fails the server might give a hint in the ExtendedInfo field
            else:
                if req_text:
                    logging.info(
                        "Target %s: %s: %s",
                        self.target,
                        req_text['error']['code'],
                        req_text['error']['message']
                    )

                    if "@Message.ExtendedInfo" in req_text['error']:

                        if isinstance(req_text['error']['@Message.ExtendedInfo'], list):
                            if ("Message" in req_text['error']['@Message.ExtendedInfo'][0]):
                                logging.info(
                                    "Target %s: %s",
                                    self.target,
                                    req_text['error']['@Message.ExtendedInfo'][0]['Message']
                                )

                        elif isinstance(req_text['error']['@Message.ExtendedInfo'], dict):

                            if "Message" in req_text['error']['@Message.ExtendedInfo']:
                                logging.info(
                                    "Target %s: %s",
                                    self.target,
                                    req_text['error']['@Message.ExtendedInfo']['Message']
                                )
                        else:
                            pass

        request_duration = round(time.time() - request_start, 2)
        logging.debug("Target %s: Request duration: %s", self.target, request_duration)
        return server_response

    def collect(self):
        if self.metrics_type == 'health':
            up_metrics = GaugeMetricFamily(
                "redfish_up",
                "Redfish Server Monitoring availability",
                labels = self.labels,
            )
            up_metrics.add_sample(
                "redfish_up", 
                value = self._redfish_up, 
                labels = self.labels
            )
            yield up_metrics

            response_metrics = GaugeMetricFamily(
                "redfish_response_duration_seconds",
                "Redfish Server Monitoring response time",
                labels = self.labels,
            )
            response_metrics.add_sample(
                "redfish_response_duration_seconds",
                value = self._response_time,
                labels = self.labels,
            )
            yield response_metrics
            
        if self._redfish_up == 0:
            return

        if self.metrics_type == 'health':

            logging.debug("Target %s: Starting health metrics collection", self.target)
            metrics = HealthCollector(self)
            metrics.collect()
            yield metrics.health_metrics

        # Finish with calculating the scrape duration
        duration = round(time.time() - self._start_time, 2)
        logging.info(
            "Target %s: %s scrape duration: %s seconds",
            self.target, self.metrics_type, duration
        )
        scrape_metrics = GaugeMetricFamily(
            f"redfish_{self.metrics_type}_scrape_duration_seconds",
            f"Redfish Server Monitoring redfish {self.metrics_type} scrabe duration in seconds",
            labels = self.labels,
        )

        scrape_metrics.add_sample(
            f"redfish_{self.metrics_type}_scrape_duration_seconds",
            value = duration,
            labels = self.labels,
        )
        yield scrape_metrics

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug("Target %s: Deleting Redfish session with server %s", self.target, self.host)
        
        response = None

        if self._auth_token:
            session_url = f"https://{self.target}:{self.rf_port}{self._session_url}"
            headers = {"x-auth-token": self._auth_token}
            logging.debug("Target %s: Deleting session at URL: %s", self.target, session_url)    
        else:
            logging.debug(
                "Target %s: No Redfish session existing with server %s",
                self.target,
                self.host
            )

        if self._session:
            logging.debug("Target %s: Closing requests session.", self.target)
            self._session.close()