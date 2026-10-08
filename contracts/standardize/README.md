# Standardize Contracts

Standardize contracts describe canonical, source-independent entity grains, keys, relationships, and point-in-time expectations.

Current state: the implemented OHLCV Standardize contract is expressed through `analytics/dbt/models/standardize/transaction/schema.yml`, model SQL, and singular dbt tests. No separate Standardize YAML contract is implemented yet.

Do not create duplicate contracts merely to fill this directory. Add an explicit cross-tool contract here only when another runtime or consumer needs a stable contract outside dbt.
