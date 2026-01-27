mkdir -p /app/imports
cat <<'{{IMPORTS_MARKER}}' > /app/imports.json
{{IMPORTS_JSON}}
{{IMPORTS_MARKER}}
{{IMPORTS_DOWNLOADER}}
