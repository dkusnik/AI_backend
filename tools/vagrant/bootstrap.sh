#!/usr/bin/env bash

set -e

echo "===== Updating system ====="
sudo apt update -y
sudo apt upgrade -y

echo "===== Installing system packages ====="
sudo apt install -y python3 python3-venv python3-pip git build-essential \
    libpq-dev redis-server postgresql postgresql-contrib mc vim

echo "===== Configuring Redis ====="
sudo systemctl enable redis-server
sudo systemctl start redis-server

echo "===== Configuring PostgreSQL ====="
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='vagrant'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE USER vagrant WITH PASSWORD 'vagrant';"

sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='AI_backend'" | grep -q 1 || \
    sudo -u postgres createdb -O vagrant AI_backend

echo "===== Installing docker ====="
# Remove old versions if exist
sudo apt remove -y docker docker-engine docker.io containerd runc || true

# Install dependencies
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release

# Add Docker GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Add Docker repository for Debian
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

echo "===== Preparing project environment ====="
cd /AI_backend

if [ ! -d "venv" ]; then
    python3 -m venv .venv
fi

source .venv/bin/activate

echo "===== Installing pip requirements ====="
pip install -r requirements.txt

echo "===== Applying Django migrations ====="
python manage.py migrate

echo "===== All done ====="
