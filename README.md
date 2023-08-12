# gov-nadlan-fetcher

Fetches real estate listings in Israel from nadlan.gov.il.
This is a quick and dirty code that only wraps around their APIs and allows for a quick fetch of the latest apartment
sales for later analysis.

There is some intense rate limiting and you can easily reach hundreds of pages in a single fetch, so try to limit the
fetch and maybe tweak the sleep between fetches.

Also sorry the code looks terrible, wrote it in an hour
