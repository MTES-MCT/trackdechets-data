#!/bin/bash

CONFIG_FILE="/srv/trackdechets-data/scripts/ssh/tunnels.conf"
SSH_KEY="/srv/ssh_keys/id_td_platform_data"
SSH_USER="git"
SSH_HOST="ssh.osc-secnum-fr1.scalingo.com"
LOG_FILE="/var/log/autossh_tunnels.log"

# Fonction pour lancer un tunnel SSH avec autossh
launch_tunnel() {
    local local_port=$1
    local remote_host=$2
    local remote_port=$3

    AUTOSSH_PIDFILE="/tmp/autossh-${local_port}.pid"

    if [ -f "$AUTOSSH_PIDFILE" ] && kill -0 "$(cat "$AUTOSSH_PIDFILE")" 2>/dev/null; then
        echo "$(date) - Tunnel on port $local_port already running." >> "$LOG_FILE"
        return
    fi

    echo "$(date) - Launching tunnel: $local_port -> $remote_host:$remote_port" >> "$LOG_FILE"

    AUTOSSH_GATETIME=0 \
    AUTOSSH_LOGLEVEL=0 \
    AUTOSSH_PIDFILE="$AUTOSSH_PIDFILE" \
    autossh -M 0 -f -N \
        -o "ExitOnForwardFailure=yes" \
        -o "ServerAliveInterval=30" \
        -o "ServerAliveCountMax=3" \
        -i "$SSH_KEY" \
        -L "0.0.0.0:${local_port}:${remote_host}:${remote_port}" \
        "${SSH_USER}@${SSH_HOST}" >> "$LOG_FILE" 2>&1

    echo "$(date) - Tunnel started on port $local_port" >> "$LOG_FILE"
}

# Lire le fichier de configuration et lancer les tunnels
while IFS= read -r line; do
    local_port=$(echo "$line" | awk '{print $1}')
    remote_host=$(echo "$line" | awk '{print $2}')
    remote_port=$(echo "$line" | awk '{print $3}')

    launch_tunnel "$local_port" "$remote_host" "$remote_port"
done < "$CONFIG_FILE"