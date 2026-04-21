#!/usr/bin/env bash
set -euo pipefail

sudo apt update
sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jre-headless unzip curl

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
echo "export PATH=\"\$JAVA_HOME/bin:\$PATH\""
echo "Ambiente preparado."
