# Legacy maintenance scripts (v1 data model)

These scripts work on the v1 model (`commodity_alias_lookup`, `markets_commodities.commodity_alias_lookup_id`,
`market_id` guessed by name). They attach names to crop groups or mandis by look-alike matching, which is
exactly what the v2 model removes.

**Do not run them against the v2 database.** In v2, crop and mandi names are matched only through
approved rows in `crop_names` and `mandi_names`, edited on the review website.
They are kept here for reference and for the v1 database until it is retired.
