# Metabase Trackdéchets

## Prérequis

Metabase est déployé comme un service conteneurisé avec un fichier `Docker Compose`.

Pour fonctionner, Metabase a besoin d'un fichier `.env`, un exemple des variables d'environnement nécessaires sont
présentes dans le fichier `.env.dist`.

Enfin Metabase est exposé sur internet grâce à [Caddy](https://caddyserver.com/) qui agit comme un reverse proxy.

## Lancer Metabase

```bash
docker compose up
```

## Caddy

[Caddy](https://caddyserver.com/) est utilisé comme reverse proxy sur l'instance de production.
Caddy est configuré à l'aide du `Caddyfile` présent dans ce dossier.

Une authentification HHTP basique est mise en place est nécessite que les variables d'environnement `HTTP_USERNAME` et `HTTP_PASSWORD_HASH` soient accessibles au lancement de Caddy.

Pour lancer Caddy, il suffit de lancer cette commande dans le même dossier que le `Caddyfile` :

```bash
caddy start
```

En cas de changement de configuration, pour la prise en compte de celui-ci :

```bash
caddy reload
```
