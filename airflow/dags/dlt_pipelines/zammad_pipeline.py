import dlt
import requests
from datetime import datetime, timedelta
from typing import Iterable, Optional
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

# Set up logging
import logging

logger = logging.getLogger("zammad_pipeline")
logger.setLevel(logging.INFO)

# Disable logging for RESTClient
logging.getLogger("your_logger_name").setLevel(logging.CRITICAL)

# Retrieve the Zammad API token and base URL from DLT secrets
API_TOKEN = dlt.secrets.get("ZAMMAD_API_TOKEN", str)
BASE_URL = dlt.secrets.get("ZAMMAD_BASE_URL", str)


def make_request(path: str, params: Optional[dict] = None) -> requests.Response:
    """Makes an authenticated request to the Zammad API."""
    headers = {
        "Content-Type": "application/json",
    }
    url = f"{BASE_URL}{path}"
    client = RESTClient(
        base_url=BASE_URL,
        headers=headers,
        auth=BearerTokenAuth(token=API_TOKEN),
        data_selector="data",
    )
    response = client.get(url, params=params)
    return response


def handle_pagination(
    response: requests.Response,
    params: dict,
    max_per_page: int,
    response_extractor: callable = lambda x: x,
) -> bool:
    """Handles pagination and adjusts for 10k limit. Returns true if there is more pages to fetch and updates params dictionary inplace."""
    current_page = params["page"]

    # Detect if we've hit the 10,000 results limit
    if (current_page * max_per_page) % 10_000 == 0:
        last_datetime = get_last_updated_at_from_response(response, response_extractor)

        if params["query"] != "updated_at:>1970-01-01":  # Initial value
            updated_at_datetime = datetime.strptime(params["query"][12:], "%Y-%m-%d")
            if last_datetime.date() == updated_at_datetime.date():
                logger.warning(
                    (
                        "It seems there were too many updates on the same day. "
                        "The paginator will move forward to the next day."
                    )
                )
                last_datetime += timedelta(days=1)
        else:
            last_datetime -= timedelta(days=1)
        # Reset pagination and update the query filter
        params["page"] = 1
        params["query"] = f"updated_at:>{last_datetime:%Y-%m-%d}"
        return True  # Continue fetching with updated params

    # Check if there are more pages
    data = response_extractor(response.json())
    if len(data) < max_per_page:
        return False  # No more pages to fetch

    # Increment the page number for the next fetch
    params["page"] += 1
    return True


def get_last_updated_at_from_response(
    response: requests.Response, response_extractor: callable
) -> datetime:
    """Extracts the latest updated_at field from the response."""
    results = response_extractor(response.json())
    if not results:
        return datetime.min  # No results, return the earliest datetime
    last_updated_at = max(
        datetime.strptime(result["updated_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
        for result in results
    )
    return last_updated_at


@dlt.resource(
    write_disposition="merge", primary_key="id", max_table_nesting=0, parallelized=True
)
def tickets(
    max_per_page: int = 200,
    updated_at=dlt.sources.incremental(
        "updated_at", initial_value="1970-01-01T00:00:00Z"
    ),
) -> Iterable[dict]:
    """Fetch tickets from the Zammad API."""
    path = "/tickets/search"

    updated_at = datetime.fromisoformat(updated_at.last_value) - timedelta(days=1)
    params = {
        "query": f"updated_at:>{updated_at:%Y-%m-%d}",
        "page": 1,
        "sort_by": "updated_at",
        "order_by": "asc",
        "per_page": max_per_page,
    }

    while True:
        response = make_request(path, params=params)
        response_json = response.json()
        if isinstance(response_json, dict):
            data = response.json().get("assets", {}).get("Ticket", {}).values()
        else:
            data = response_json

        for ticket in data:
            ticket = {**ticket, "tags": tags(ticket)}
            yield ticket

        if not handle_pagination(
            response,
            params,
            max_per_page,
            response_extractor=lambda x: x,
        ):
            break


@dlt.resource(
    write_disposition="merge", primary_key="id", max_table_nesting=0, parallelized=True
)
def users(
    max_per_page: int = 200,
    updated_at=dlt.sources.incremental(
        "updated_at", initial_value="1970-01-01T00:00:00Z"
    ),
) -> Iterable[dict]:
    """Fetch users from the Zammad API."""
    path = "/users/search"

    updated_at = datetime.fromisoformat(updated_at.last_value) - timedelta(days=1)
    params = {
        "query": f"updated_at:>{updated_at:%Y-%m-%d}",
        "page": 1,
        "sort_by": "updated_at",
        "order_by": "asc",
        "per_page": max_per_page,
    }

    while True:
        response = make_request(path, params=params)
        data = response.json()
        for user in data:
            yield user

        if not handle_pagination(response, params, max_per_page):
            break


@dlt.resource(
    write_disposition="replace",
    primary_key="id",
    max_table_nesting=0,
    parallelized=True,
)
def groups() -> Iterable[dict]:
    """Fetch groups from the Zammad API."""
    path = "/groups"
    response = make_request(path)
    for group in response.json():
        yield group


@dlt.resource(
    write_disposition="merge", primary_key="id", max_table_nesting=0, parallelized=True
)
def organizations(
    max_per_page: int = 200,
    updated_at=dlt.sources.incremental(
        "updated_at", initial_value="1970-01-01T00:00:00Z"
    ),
) -> Iterable[dict]:
    """Fetch organizations from the Zammad API."""
    path = "/organizations/search"

    updated_at = datetime.fromisoformat(updated_at.last_value) - timedelta(days=1)
    params = {
        "query": f"updated_at:>{updated_at:%Y-%m-%d}",
        "page": 1,
        "sort_by": "updated_at",
        "order_by": "asc",
        "per_page": max_per_page,
    }

    while True:
        response = make_request(path, params=params)
        data = response.json()
        for organization in data:
            yield organization

        if not handle_pagination(response, params, max_per_page):
            break


@dlt.resource(
    write_disposition="merge", primary_key="id", max_table_nesting=0, parallelized=True
)
def text_modules(
    max_per_page: int = 200,
    updated_at=dlt.sources.incremental(
        "updated_at", initial_value="1970-01-01T00:00:00Z"
    ),
) -> Iterable[dict]:
    """Fetch organizations from the Zammad API."""
    path = "/text_modules/search"

    updated_at = datetime.fromisoformat(updated_at.last_value) - timedelta(days=1)
    params = {
        "query": f"updated_at:>{updated_at:%Y-%m-%d}",
        "page": 1,
        "sort_by": "updated_at",
        "order_by": "asc",
        "per_page": max_per_page,
    }

    while True:
        response = make_request(path, params=params)
        data = response.json()
        for text_module in data:
            yield text_module

        if not handle_pagination(response, params, max_per_page):
            break


def tags(ticket_item: dict) -> list:
    """Fetch tags for each ticket."""
    path = "/tags"

    params = {"object": "Ticket", "o_id": ticket_item["id"]}
    response = make_request(path, params=params)
    return response.json().get("tags", [])


@dlt.source
def zammad_source():
    return [tickets, users, groups, organizations, text_modules]


# Run the pipeline
if __name__ == "__main__":
    # Initialize the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="zammad_pipeline",
        destination="clickhouse",
        progress="tqdm",
    )

    # Extract and load data
    load_info = pipeline.run(zammad_source())
    logger.info(f"Pipeline finished with load info: {load_info}")
