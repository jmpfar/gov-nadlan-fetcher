# gov-nadlan-fetcher

Fetches real estate listings in Israel from nadlan.gov.il.
This is a quick and dirty code that only wrap around their APIs and allow for a quick fetch of the latest apartment
sales for later analysis.

There is some intense rate limiting there and you can easily fetch hundreds of pages, so try to limit the
fetch and maybe tweak the sleep between fetches.

Also sorry the code looks terrible, wrote it in a hour
