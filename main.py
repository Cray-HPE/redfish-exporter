from handler import metricsHandler
from handler import welcomePage

from wsgiref.simple_server import make_server, WSGIServer, WSGIRequestHandler
from socketserver import ThreadingMixIn
import falcon

import argparse
import yaml
import logging
import os
import warnings
import sys

class _SilentHandler(WSGIRequestHandler):
    """WSGI handler that does not log requests."""

    def log_message(self, format, *args):
        """Log nothing."""

class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    """Thread per request HTTP server."""

def falcon_app(config):
    port = int(os.getenv("LISTEN_PORT", config.get("listen_port", 9200)))
    addr = "0.0.0.0"
    logging.info("Starting Redfish Prometheus Server on Port %s", port)

    api = falcon.API()
    api.add_route("/health",  metricsHandler(config, metrics_type='health'))
    api.add_route("/", welcomePage())

    with make_server(addr, port, api, ThreadingWSGIServer, handler_class=_SilentHandler) as httpd:
        httpd.daemon = True
        logging.info("Listening on Port %s", port)
        try:
            httpd.serve_forever()
        except (KeyboardInterrupt, SystemExit):
            logging.info("Stopping Redfish Prometheus Server")

def enable_logging(filename, debug):
    
    logger = logging.getLogger()
    
    formatter = logging.Formatter('%(asctime)-15s %(process)d %(filename)24s:%(lineno)-3d %(levelname)-7s %(message)s')

    if debug:
        logger.setLevel("DEBUG")
    else:
        logger.setLevel("INFO")

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    if filename:
        try:
            fh = logging.FileHandler(filename, mode='w')
        except FileNotFoundError as e:
            logging.error(f"Could not open logfile {filename}: {e}")
            sys.exit(1)

        fh.setFormatter(formatter)
        logger.addHandler(fh)

def get_args():
    """
    Get the command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        help="Specify config yaml file",
        metavar="FILE",
        required=False,
        default="/config/config.yml"
    )
    parser.add_argument(
        "-l",
        "--logging",
        help="Log all messages to a file",
        metavar="FILE",
        required=False
    )
    parser.add_argument(
        "-d", "--debug",
        help="Debugging mode",
        action="store_true",
        required=False
    )

    return parser.parse_args()

if __name__ == "__main__":

    args = get_args()

    warnings.filterwarnings("ignore")

    enable_logging(args.logging, args.debug)

    # get the config

    if args.config:
        try:
            with open(args.config, "r", encoding="utf8") as config_file:
                configuration = yaml.load(config_file.read(), Loader=yaml.FullLoader)
        except FileNotFoundError as err:
            print(f"Config File not found: {err}")
            sys.exit(1)

        falcon_app(configuration)
