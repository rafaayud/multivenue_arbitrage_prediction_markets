#!/bin/sh
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io docker-compose-v2
systemctl enable --now docker
usermod -aG docker ubuntu
install -d -m 700 -o ubuntu -g ubuntu /home/ubuntu/prediction-bot
# Small emergency swap absorbs startup peaks; normal trading should stay in RAM.
if [ ! -f /swapfile ]; then
  fallocate -l 1G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
echo 'vm.swappiness=10' > /etc/sysctl.d/90-prediction-bot.conf
sysctl -p /etc/sysctl.d/90-prediction-bot.conf
