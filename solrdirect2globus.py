"""A script for ingesting Solr records into Globus.

Warning: In order to progressively yield records in the map pool, exceptions that are
thrown in the `ingest_chunk()` routine are eaten. I recommend using the globus cli:

globus search task list GLOBUS_INDEX_ID

If you aren't seeing tasks populated, something is wrong.

"""

import logging
import math
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, _base
from urllib.parse import urljoin

from globus_sdk import (
    AccessTokenAuthorizer,
    NativeAppAuthClient,
    SearchClient,
    SearchQuery,
)
from pysolr import Solr
from tqdm import tqdm

SOLR_URL = "http://esgf-data-node-solr-write:8983/solr/"
CHUNK_SIZE = 1000  # Globus rejected submissions much larger
SEARCH_QUERY = "*"
GLOBUS_INDEX_ID = "ea4595f4-7b71-4da7-a1f0-e3f5d8f7f062"
CHECK = True  # Check if the ids already exist

logging.basicConfig(
    filename="ingest.log",
    format="%(asctime)s %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
)


def _result_or_cancel(fut, timeout=None):
    try:
        try:
            return fut.result(timeout)
        finally:
            fut.cancel()
    finally:
        # Break a reference cycle with the exception in self._exception
        del fut


class PoisonPill:
    pass


class FixedExecutor(_base.Executor):
    def map(self, fn, *iterables, timeout=None, chunksize=1):
        if timeout is not None:
            end_time = timeout + time.monotonic()

        poison_pill = PoisonPill()
        fs = queue.Queue()

        def submit():
            for args in zip(*iterables):
                fs.put(self.submit(fn, *args))
            fs.put(poison_pill)

        submission_thread = threading.Thread(target=submit)

        def result_iterator():
            future = None
            try:
                while True:
                    future = fs.get()
                    if future == poison_pill:
                        return
                    if timeout is None:
                        res = _result_or_cancel(future)
                        yield res
                    else:
                        res = _result_or_cancel(future, end_time - time.monotonic())
                        yield res
            finally:
                if future and future == poison_pill:
                    return
                while True:
                    future = fs.get()
                    if future == poison_pill:
                        return
                    future.cancel()

        submission_thread.start()
        yield from result_iterator()


ThreadPoolExecutor.__bases__ = (FixedExecutor,)


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
    auth_code = input(f"Paste the globus auth code from {authorize_url}").strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    auth = AccessTokenAuthorizer(token_response["access_token"])
    return auth


def iter_chunks():
    with tqdm(
        desc="Fetching first chunk of Files...", unit="chunk"
    ) as collection_progress:
        solr = Solr(urljoin(SOLR_URL, "files"))
        search_results = solr.search(
            SEARCH_QUERY,
            rows=CHUNK_SIZE,
            hl="false",
            cursorMark="*",
            sort="id asc",
            facet="false",
            fl="*",
        )
        collection_progress.total = math.ceil(search_results.hits / CHUNK_SIZE)
        while search_results:
            yield search_results.docs
            collection_progress.set_description("Fetching new Files chunk...")
            collection_progress.update(1)
            search_results = (
                search_results._next_page_query and search_results._next_page_query()
            )


def ingest_chunk(args):
    chunk, client = args
    # If all the ids are ingested, just skip. This will just keep Globus' ingest queue
    # from getting plugged up with things that we have already done.
    tqdm.write("ingest_begin")
    if CHECK:
        ids = [doc["id"] for doc in chunk]
        tqdm.write(f"found ids {len(ids)}")
        response = SearchClient().post_search(
            GLOBUS_INDEX_ID,
            SearchQuery("")
            .add_filter("type", ["File"])
            .add_filter("id", ids, type="match_any"),
            limit=CHUNK_SIZE,
        )
        tqdm.write(f"globus response {response.http_status}")
        if response.http_status == 200 and response["count"] == len(ids):
            tqdm.write("ids exist already, skipping")
            return response.data
    tqdm.write("ids do not exist, ingesting")
    response = client.ingest(
        GLOBUS_INDEX_ID,
        {
            "ingest_type": "GMetaList",
            "ingest_data": {
                "gmeta": [
                    {
                        "id": "file",
                        "subject": solr_doc["id"],
                        "visible_to": ["public"],
                        "content": solr_doc,
                    }
                    for solr_doc in chunk
                ],
            },
        },
    )
    if not (response.data["acknowledged"] and response.data["success"]):
        tqdm.write("failed to ingest")
        logging.debug(f"FAIL {response.data['task_id']}")
        raise ValueError
    tqdm.write("ingest successful")
    logging.debug(f"ACCEPT {response.data['task_id']}")
    return response.data


elastic_client = SearchClient(authorizer=get_authorization())
with ThreadPoolExecutor() as pool:
    for result in pool.map(
        ingest_chunk, ((chunk, elastic_client) for chunk in iter_chunks())
    ):
        result.keys()  # just do something with the output
        del result
