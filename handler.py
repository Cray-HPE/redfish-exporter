import falcon
import logging
import socket
import re
import os
import traceback

from prometheus_client.exposition import CONTENT_TYPE_LATEST
from prometheus_client.exposition import generate_latest

from collector import RedfishMetricsCollector

class welcomePage:
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.content_type = 'text/html'
        resp.text = """
        <h1>Redfish Exporter</h1>
        <h2>Prometheus Exporter for redfish API based servers monitoring</h2>
        """

class metricsHandler:
    def __init__(self, config, metrics_type):
        self._config = config
        self.metrics_type = metrics_type

    def on_get(self, req, resp):
        target = req.get_param("target")
        if not target:
            logging.error("No target parameter provided!")
            raise falcon.HTTPMissingParam("target")

        logging.debug(f"Received Target %s for metrics type: %s", target, self.metrics_type)

        ip_re = re.compile(
            r"^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}"
            r"([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"
        )

        resp.set_header("Content-Type", CONTENT_TYPE_LATEST)
        
        host = None

        if ip_re.match(target):
            logging.debug("Target %s: Target is an IP Address.", target)
            try:
                host = socket.gethostbyaddr(target)[0]
            except socket.herror as err:
                logging.warning("Target %s: Reverse DNS lookup failed: %s. Using IP address as host.", target, err)
                host = target
        else:
            logging.debug("Target %s: Target is a hostname.", target)
            host = target
            try:
                target = socket.gethostbyname(host)
            except socket.gaierror as err:
                msg = f"Target {target}: DNS lookup failed: {err}"
                logging.error(msg)
                raise falcon.HTTPInvalidParam(msg, "target")

        usr = self._config.get("username")
        pwd = self._config.get("password")
        rf_port = self._config.get("rf_port")

        logging.debug("Target %s: Using user %s with port %s", target, usr, rf_port)

        with RedfishMetricsCollector(
            self._config,
            target = target,
            host = host,
            usr = usr,
            pwd = pwd, 
            rf_port = rf_port,
            metrics_type = self.metrics_type
        ) as registry:

            # open a session with the remote board
            registry.get_session()

            try:
                # collect the actual metrics
                logging.debug("Target %s: Collecting %s metrics", target, self.metrics_type)
                resp.text = generate_latest(registry)
                resp.status = falcon.HTTP_200
                logging.debug("Target %s: Successfully generated %s metrics", target, self.metrics_type)

            except Exception as err:
                message = f"Exception: {traceback.format_exc()}"
                logging.error("Target %s: %s", target, message)
                raise falcon.HTTPBadRequest(description=message)
