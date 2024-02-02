# solr2globus

This utility script is used to query a Solr-based ESGF index, harvest the metadata information, and then submit it to a Globus-based index. This utility does not need to be installed. You will just need to install the dependencies.

```bash
pip install -r requirements.txt
```

## Notes

- You must have `owner`, `admin`, or `writer` access to the `globus_index_id` to which you wish to ingest. 
- The utility will use the `backoff` library to continue to make the required requests to both indices until they are successful, gradually increasing the time between requests.
- As we anticipate running this utility relatively few times, you will need to authenticate for each execution. If annoying, we could add refresh token support.
- You may ingest by searches using `esg-search` syntax. A sample query is provided which took ~30 seconds to submit the ingest request.
- This utility does not monitor when the Globus ingest tasks have completed--we just ensure that they were accepted. There can be a time lag between when you make the request and when your entries will appear in the new index.
