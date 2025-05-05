# Tunnels SSH de la Plateforme Data

Le script `launch_tunnel.sh` permet de lancer les tunnels nécessaires aux fonctionnement de la Plateforme Data. Pour lancer le script,
il est nécessaire au préalable d'avoir installer `autossh` (`sudo apt-get install autossh`).

Ensuite il faut créer un fichier `tunnels.conf` dans lequel chaque ligne correspond à la configuration pour un tunnel SSH. Par exemple:

```
10000 un-serveur.postgresql.dbs.scalingo.com 30755
10001 un-autre-serveur.postgresql.a.osc-secnum-fr1.scalingo-dbs.com 30999
```

Le premier nombre de la ligne correspond au port local sur lequel sera ouvert le port, ensuite vient l'hôte du serveur distant et finalement le port SSH du serveur distant.

## Configuration du lancement automatique au démarrage de l'instance

Pour configurer `systemd` afin de lancer les tunnels au démarrage de l'instance, il faut d'abord créer un fichier pour déclarer le service :

```bash
sudo nano /etc/systemd/system/autossh-tunnels.service
```

Dans ce fichier il faut écrire la configuration suivante :

```
[Unit]
Description=AutoSSH Tunnel Manager
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/srv/trackdechets-data/scripts/ssh/launch_tunnel.sh
Restart=on-failure
RestartSec=5
User=root
Environment=AUTOSSH_DEBUG=1
StandardOutput=append:/var/log/autossh_tunnels.log
StandardError=append:/var/log/autossh_tunnels.log
Type=oneshot
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
```

Ensuite il ne reste plus qu'à appliquer les modifications :

```bash
sudo systemctl daemon-reload
sudo systemctl restart autossh-tunnels.service
```
