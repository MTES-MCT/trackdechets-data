#!/bin/bash

CONFIG_FILE="tunnels.conf"
SSH_KEY="/opt/ssh_keys/id_scalingo_scaleway_instance"
SSH_USER="git"
SSH_HOST="ssh.osc-secnum-fr1.scalingo.com"

# Fonction pour lancer un tunnel SSH
launch_tunnel() {
    local local_port=$1
    local remote_host=$2
    local remote_port=$3

    ssh -L 0.0.0.0:${local_port}:${remote_host}:${remote_port} -i ${SSH_KEY} ${SSH_USER}@${SSH_HOST} -NTf
}

# Fonction pour vérifier si un tunnel est actif
is_tunnel_active() {
    local local_port=$1
    nc -zv 0.0.0.0 ${local_port} &>/dev/null
}

# Lire le fichier de configuration et lancer les tunnels
while IFS= read -r line; do
    local_port=$(echo $line | awk '{print $1}')
    remote_host=$(echo $line | awk '{print $2}')
    remote_port=$(echo $line | awk '{print $3}')

    launch_tunnel $local_port $remote_host $remote_port
done < "$CONFIG_FILE"

# Surveiller les tunnels et les relancer en cas de crash
while true; do
    while IFS= read -r line; do
        local_port=$(echo $line | awk '{print $1}')

        if ! is_tunnel_active $local_port; then
            remote_host=$(echo $line | awk '{print $2}')
            remote_port=$(echo $line | awk '{print $3}')
            launch_tunnel $local_port $remote_host $remote_port
        fi
    done < "$CONFIG_FILE"

    sleep 60  # Vérifier toutes les 60 secondes
done