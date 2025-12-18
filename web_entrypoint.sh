#!/usr/bin/env bash
set -e

echo "Running collectstatic..."
python manage.py collectstatic --noinput

echo "Running migrations..."
python manage.py migrate --noinput

echo "Starting Django..."
exec "$@"
