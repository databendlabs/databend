#!/bin/sh
set -e

# Add Databend to adm group to read /var/logs
usermod --append --groups adm databend || true

if getent group 'systemd-journal'; then
	# Add Databend to systemd-journal to read journald logs
	usermod --append --groups systemd-journal databend || true
	systemctl daemon-reload || true
fi

if getent group 'systemd-journal-remote'; then
	# Add Databend to systemd-journal-remote to read remote journald logs
	usermod --append --groups systemd-journal-remote databend || true
	systemctl daemon-reload || true
fi
