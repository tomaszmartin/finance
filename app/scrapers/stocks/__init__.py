"""Extracting data from GPW."""
import requests

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += "HIGH:!DH:!aNULL"  # type: ignore
