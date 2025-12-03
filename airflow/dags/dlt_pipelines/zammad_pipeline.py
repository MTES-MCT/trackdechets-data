import dlt
import requests
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from airflow.operators.python import get_current_context
from dlt.pipeline.mark import make_nested_hints


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


def _set_dlt_updated_at_filter(context, dlt_updated_at) -> str:
    """
    Set the updated_at filter for the DLT pipeline.
    If the updated_at is provided in the context, use it to set the updated_at filter.
    Otherwise, use the last value of the DLT pipeline.
    """
    try:
        updated_at = (
            datetime.fromisoformat(context["dag_run"].conf.get("start_date"))
            if context and context.get("dag_run")
            else None
        )
        logger.info(f"Force updated at: {updated_at} from context")
    except Exception:
        updated_at = datetime.fromisoformat(dlt_updated_at.last_value) - timedelta(
            days=1
        )
    return updated_at


def _set_dlt_end_date_filter(context) -> datetime:
    """
    Set the end_date filter for the DLT pipeline.
    If the end_date is provided in the context, use it to set the end_date filter.
    Otherwise, use the last value of the DLT pipeline.
    """
    try:
        end_date = (
            datetime.fromisoformat(context["dag_run"].conf.get("end_date"))
            if context and context.get("dag_run")
            else None
        )
        logger.info(f"Force end date: {end_date} from context")
    except Exception:
        end_date = None
    return end_date


@dlt.resource(
    write_disposition="merge",
    primary_key="id",
    parallelized=True,
    max_table_nesting=1,
    nested_hints={
        "articles": make_nested_hints(
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "ticket_id", "data_type": "bigint"},
                {"name": "type_id", "data_type": "bigint"},
                {"name": "sender_id", "data_type": "bigint"},
                {"name": "from", "data_type": "text"},
                {"name": "to", "data_type": "text"},
                {"name": "cc", "data_type": "text", "nullable": True},
                {"name": "subject", "data_type": "text", "nullable": True},
                {"name": "reply_to", "data_type": "text", "nullable": True},
                {"name": "message_id", "data_type": "text"},
                {"name": "message_id_md5", "data_type": "text"},
                {"name": "in_reply_to", "data_type": "text", "nullable": True},
                {"name": "content_type", "data_type": "text"},
                {"name": "body", "data_type": "text"},
                {"name": "internal", "data_type": "bool"},
                {"name": "preferences", "data_type": "complex", "nullable": True},
                {"name": "updated_by_id", "data_type": "bigint"},
                {"name": "created_by_id", "data_type": "bigint"},
                {"name": "origin_by_id", "data_type": "bigint", "nullable": True},
                {"name": "created_at", "data_type": "timestamp"},
                {"name": "updated_at", "data_type": "timestamp"},
                {"name": "detected_language", "data_type": "text", "nullable": True},
                {"name": "attachments", "data_type": "complex", "nullable": True},
                {"name": "created_by", "data_type": "text"},
                {"name": "updated_by", "data_type": "text"},
            ]
        )
    },
)
def tickets(
    max_per_page: int = 200,
    updated_at=dlt.sources.incremental(
        "updated_at", initial_value="1970-01-01T00:00:00Z"
    ),
) -> Iterable[dict]:
    """Fetch tickets from the Zammad API."""
    path = "/tickets/search"

    updated_at = _set_dlt_updated_at_filter(
        context=get_current_context(), dlt_updated_at=updated_at
    )
    last_updated_at = updated_at
    end_date = _set_dlt_end_date_filter(context=get_current_context())

    params = {
        "query": f"updated_at:>{updated_at:%Y-%m-%d}",
        "page": 1,
        "sort_by": "updated_at",
        "order_by": "asc",
        "per_page": max_per_page,
    }

    while True:
        logger.info(
            f"Last updated at: {last_updated_at.date()}, End date: {end_date.date() if end_date else 'None'}"
        )
        if end_date and last_updated_at.date() > end_date.date():
            logger.info("Reached end date, stopping the pipeline")
            break
        response = make_request(path, params=params)
        response_json = response.json()
        if isinstance(response_json, dict):
            data = response.json().get("assets", {}).get("Ticket", {}).values()
        else:
            data = response_json

        for ticket in data:
            ticket = {
                **ticket,
                "tags": tags(ticket),
                "articles": articles_by_ticket(ticket),
            }
            last_updated_at = datetime.fromisoformat(ticket["updated_at"]).replace(
                tzinfo=timezone.utc
            )
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

    updated_at = _set_dlt_updated_at_filter(
        context=get_current_context(), dlt_updated_at=updated_at
    )

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

    updated_at = _set_dlt_updated_at_filter(
        context=get_current_context(), dlt_updated_at=updated_at
    )

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

    updated_at = _set_dlt_updated_at_filter(
        context=get_current_context(), dlt_updated_at=updated_at
    )

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


def articles_by_ticket(ticket_item: dict) -> list:
    """Fetch ticket articles for each ticket."""
    path = f"/ticket_articles/by_ticket/{ticket_item['id']}"

    response = make_request(path)
    articles = response.json()
    return articles


@dlt.source
def zammad_source():
    return [
        tickets(),
        users(),
        groups(),
        organizations(),
        text_modules(),
    ]


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
