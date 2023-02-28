#!/bin/sh
set -e

# Add databend:databend user & group
id --user databend >/dev/null 2>&1 ||
	useradd --system --shell /sbin/nologin --home-dir /var/lib/databend --user-group \
		--comment "Databend cloud data analytics" databend

# Create default Databend data directory
mkdir -p /var/lib/databend

# Make databend:databend the owner of the Databend data directory
chown -R databend:databend /var/lib/databend

# Create default Databend log directory
mkdir -p /var/log/databend

# Make databend:databend the owner of the Databend log directory
chown -R databend:databend /var/log/databend
