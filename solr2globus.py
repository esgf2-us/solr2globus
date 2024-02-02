import datetime
import logging
import time

import backoff
import requests
from globus_sdk import (
    AccessTokenAuthorizer,
    GlobusError,
    NativeAppAuthClient,
    SearchClient,
)
from tqdm import tqdm


def get_authorization():
    """Get an authorizer that will allow the ingest.

    You will need to be the owner/admin of the globus index into which you wish to
    ingest. The client uuid I have hard-coded in here is one I registered under the
    ESGF2 project for solr2globus. As far as I know, this just gives Globus a
    responsible application when looking in logs.
    """
    client = NativeAppAuthClient("cb4f0bba-e44c-4a36-9023-06929dbb4742")
    client.oauth2_start_flow(
        requested_scopes=["urn:globus:auth:scope:search.api.globus.org:all"],
        refresh_tokens=False,
    )
    authorize_url = client.oauth2_get_authorize_url()
    print(
        f"""
All interactions with Globus must be authorized. Go here:

{authorize_url}

and paste the link here:\n"""
    )
    auth_code = input("> ").strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    auth = AccessTokenAuthorizer(token_response["access_token"])
    return auth


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException)
def esg_search(base_url, **search):
    """Return an esg-search response as a dictionary."""
    if "format" not in search:
        search["format"] = "application/solr+json"
    response = requests.get(f"{base_url}/esg-search/search", params=search)
    response.raise_for_status()
    return response.json()


@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException, GlobusError))
def ingest(client, entries):
    response = client.ingest(
        globus_index_id,
        {
            "ingest_type": "GMetaList",
            "ingest_data": {"gmeta": entries},
        },
    )
    if not (response.data["acknowledged"] and response.data["success"]):
        logger.info(f"{response.data}")
        raise GlobusError()


def amend_doc(doc):
    """Amend the document.

    If if wish to make changes to the information obtained from the Solr index, change
    it in this routine. In our case, we want to simply replicate the document exactly.
    """
    return doc


def ingest_by_search(client, chunk_size=1000, **search):
    """Ingest the records found in the given search, `chunk_size` at a time."""
    ingest_time = time.time()
    # To know how many chunks to submits, we need to know how many results
    num_results = esg_search(solr_base_url, limit=0, **search)["response"]["numFound"]
    num_chunks = int(num_results / chunk_size) + 1
    for i in tqdm(range(num_chunks), unit="chunk"):
        data = esg_search(
            solr_base_url, offset=i * chunk_size, limit=chunk_size, **search
        )
        entries = []
        for doc in data["response"]["docs"]:
            doc = amend_doc(doc)
            gmeta_entry = {
                "id": search["type"].lower() if "type" in search else "dataset",
                "subject": doc["id"],
                "visible_to": ["public"],
                "content": doc,
            }
            entries.append(gmeta_entry)
        ingest(client, entries)
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
        format="%(asctime)s: %(message)s",
        filename="ingest.log",
    )
    logger = logging.getLogger(__name__)

    # Create a client authorized to ingest and then ingest by a search. Files and
    # Datasets must be ingested separately as esg-search does not query both types
    # simultaneously.
    client = SearchClient(authorizer=get_authorization())
    ingest_time = time.time()
    for entry_type in ["Dataset", "File"]:
        ingest_by_search(
            client,
            experiment_id="historical",
            source_id="CESM2",
            variant_label="r1i1p1f1",
            type=entry_type,
        )
    ingest_time = time.time() - ingest_time
    logger.info(f"ingest total time {str(datetime.timedelta(seconds=ingest_time))}")
