import datetime
import logging
import time
from pathlib import Path

import backoff
import globus_sdk as sdk
import requests
from globus_sdk.tokenstorage import SimpleJSONFileAdapter
from tqdm import tqdm


def get_authorized_search_client() -> sdk.SearchClient:
    """Return a transfer client authorized to make transfers."""
    config_path = Path.home() / ".config" / "solr2globus"
    config_path.mkdir(parents=True, exist_ok=True)
    token_adapter = SimpleJSONFileAdapter(config_path / "tokens.json")
    client = sdk.NativeAppAuthClient("cb4f0bba-e44c-4a36-9023-06929dbb4742")
    if token_adapter.file_exists():
        tokens = token_adapter.get_token_data("search.api.globus.org")
    else:
        client.oauth2_start_flow(
            requested_scopes=["urn:globus:auth:scope:search.api.globus.org:all"],
            refresh_tokens=True,
        )
        authorize_url = client.oauth2_get_authorize_url()
        print(
            f"""
All interactions with Globus must be authorized. To ensure that we have permission to faciliate your transfer, please open the following link in your browser.

{authorize_url}

You will have to login (or be logged in) to your Globus account. Globus will also request that you give a label for this authorization. You may pick anything of your choosing. After following the instructions in your browser, Globus will generate a code which you must copy and paste here and then hit <enter>.\n"""
        )
        auth_code = input("> ").strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)
        token_adapter.store(token_response)
        tokens = token_response.by_resource_server["search.api.globus.org"]
    authorizer = sdk.RefreshTokenAuthorizer(
        tokens["refresh_token"],
        client,
        access_token=tokens["access_token"],
        expires_at=tokens["expires_at_seconds"],
        on_refresh=token_adapter.on_refresh,
    )
    client = sdk.SearchClient(authorizer=authorizer)
    return client


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException)
def esg_search(base_url, **search):
    """Return an esg-search response as a dictionary."""
    if "format" not in search:
        search["format"] = "application/solr+json"
    response = requests.get(f"{base_url}/esg-search/search", params=search)
    response.raise_for_status()
    return response.json()


@backoff.on_exception(
    backoff.expo, (requests.exceptions.RequestException, sdk.GlobusError)
)
def ingest(client, entries):
    response = client.ingest(
        globus_index_id,
        {
            "ingest_type": "GMetaList",
            "ingest_data": {"gmeta": entries},
        },
    )
    if not (response.data["acknowledged"] and response.data["success"]):
        logger.error(f"{response.data}")
        raise sdk.GlobusError()


def amend_doc(doc):
    """Amend the document.

    If if wish to make changes to the information obtained from the Solr index, change
    it in this routine. In our case, we want to simply replicate the document exactly.
    Look for where it is called and make sure you uncomment it.
    """
    return doc


def ingest_by_search(client, chunk_size=1000, **search):
    """Ingest the records found in the given search, `chunk_size` at a time."""
    ingest_time = time.time()
    # To know how many chunks to submits, we need to know how many results
    num_results = esg_search(solr_base_url, limit=0, **search)["response"]["numFound"]
    num_chunks = int(num_results / chunk_size) + 1
    for i in tqdm(
        range(num_chunks),
        unit="chunk",
        desc=".".join([val for facet, val in search.items() if facet != "type"]),
    ):
        data = esg_search(
            solr_base_url, offset=i * chunk_size, limit=chunk_size, **search
        )
        # We may perfectly page to the last entry and thus get a 0-length docs
        if data["response"]["numFound"] == data["response"]["start"]:
            continue
        entries = []
        for doc in data["response"]["docs"]:
            # doc = amend_doc(doc)
            gmeta_entry = {
                "id": search["type"].lower() if "type" in search else "dataset",
                "subject": doc["id"],
                "visible_to": ["public"],
                "content": doc,
            }
            entries.append(gmeta_entry)
        try:
            ingest(client, entries)
        except sdk.SearchAPIError as exception:
            logger.error(f"Failed to ingest {chunk_size=} {search=} {exception=}")

    ingest_time = time.time() - ingest_time
    logger.info(
        f"ingested {num_results} records in {ingest_time:.2f} [s] at {num_results/ingest_time:.2f} record/s"
    )


if __name__ == "__main__":
    # Configure the source Solr index by URL and the target Globus index by UUID
    solr_base_url = "http://esgf-node.ornl.gov"
    globus_index_id = "ea4595f4-7b71-4da7-a1f0-e3f5d8f7f062"

    # Setup some logging, you will see Globus' logs too
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]%(asctime)s: %(message)s",
        filename="ingest.log",
    )
    logger = logging.getLogger(__name__)

    # We recommend that you ingest records by projects and across a set of facets. This
    # is because we observe that when there are too many pages of results, the
    # performance of the Solr-index via esg_search degrades substantially.
    PROJECT = "CMIP6"
    BY_FACET = "experiment_id"
    RECORD_TYPE = "File"  # or Dataset

    # The larger this is, the faster the ingest will be. However, globus has a limit on
    # the memory that can be ingested at a time. Since different projects store
    # different data in their records, you may have to reduce this if you notice in the
    # log that your request is being rejected.
    CHUNK_SIZE = 1000

    # Authorize our client to ingest
    client = get_authorized_search_client()

    # Get the facets for the project we are ingesting
    response = esg_search(
        "http://esgf-node.ornl.gov",
        type=RECORD_TYPE,
        project=PROJECT,
        facets=BY_FACET,
        limit=0,
    )
    facets = [
        e
        for _, e in sorted(
            zip(
                response["facet_counts"]["facet_fields"][BY_FACET][1::2],
                response["facet_counts"]["facet_fields"][BY_FACET][::2],
            )
        )
    ]

    # Main work loop
    for facet in facets:
        ingest_time = time.time()
        ingest_by_search(
            client,
            type=RECORD_TYPE,
            chunk_size=CHUNK_SIZE,
            project=PROJECT,
            **{BY_FACET: facet},
        )
        ingest_time = time.time() - ingest_time
        logger.info(f"ingest total time {str(datetime.timedelta(seconds=ingest_time))}")
