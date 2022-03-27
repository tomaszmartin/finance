"""This module contains common methods for all scrapers."""
import requests


def extract_content(response: requests.Response) -> bytes:
    """Extracts content from response, makes sure that the
    response is correct.

    Args:
        response: Response object.

    Returns:
        Bytes if everything goes well.

    Raises:
        HTTPError: when response is incorrect.
    """
    response.raise_for_status()
    data = response.content
    return data  # type: ignore
