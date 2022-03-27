"""Extracting data from GPW."""
import requests

# pylint: disable=no-member
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += "HIGH:!DH:!aNULL"  # type: ignore
