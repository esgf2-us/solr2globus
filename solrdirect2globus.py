import math
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, _base
from urllib.parse import urljoin, urlparse, urlunparse

import backoff
from globus_sdk import (
    AccessTokenAuthorizer,
    GlobusError,
    NativeAppAuthClient,
    SearchClient,
)
from pysolr import Solr
from tqdm import tqdm

SOLR_URL = "http://esgf-data-node-solr-write:8983/solr/"
CHUNK_SIZE = 20000
SEARCH_QUERY = "*"
GLOBUS_INDEX_ID = "ea4595f4-7b71-4da7-a1f0-e3f5d8f7f062"


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
    chunk_with_progress = tqdm(
        chunk, desc="Updating chunk...", unit="doc", miniters=1000
    )
    response = client.ingest(
        GLOBUS_INDEX_ID,
        {
            "ingest_type": "GMetaList",
            "ingest_data": {
                "GMetaList": [
                    {
                        "id": "files",
                        "subject": solr_doc["id"],
                        "visible_to": ["public"],
                        "content": solr_doc,
                    }
                    for solr_doc in chunk_with_progress
                ],
            },
        },
    )
    if not (response.data["acknowledged"] and response.data["success"]):
        print(response.data)
        raise ValueError
    tqdm.write("Submitting chunk...")
    return response.data


elastic_client = SearchClient(authorizer=get_authorization())
with ThreadPoolExecutor() as pool:
    for result in pool.map(
        ingest_chunk, ((chunk, elastic_client) for chunk in iter_chunks())
    ):
        tqdm.write(f"Submitted chunk: {result}")
        del result
