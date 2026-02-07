all: query_to_meta.txt meta_to_query.txt

generated/resolved_min_compatibles.txt: src/min_compatible_versions.txt src/external-meta-min-compatibles.txt scripts/resolve_versions.py
	python3 scripts/resolve_versions.py

query_to_meta.txt: generated/resolved_min_compatibles.txt scripts/find_compat.py
	python3 scripts/find_compat.py --all-query > $@

meta_to_query.txt: generated/resolved_min_compatibles.txt scripts/find_compat.py
	python3 scripts/find_compat.py --all-meta > $@
