# Trackdéchets ClickHouse

L'entrepôt de données Trackdéchets est basé sur Clickhouse.
Ce dossier contient la configuration Docker pour faire tourner le service.

## Réseau

Le service dépend d'un réseau docker qui le connecte avec les autres servieces de la plateforme données. Il faut créer ce réseau docker comme suit :

```bash
docker network create data_external_network
```

## Variables d'environnement

Le fichier `.env.dist` donne un exemple de variables d'environnement utilisées pour ClickHouse. Notamment les ports exposées par ClickHouse.

## Initialisation

Avant le premier démarrage de ClickHouse, il faut créer un fichier `users.d/default-user.xml` pour configurer le premier utilisateur et contenant :

```xml
<clickhouse>
    <!-- Docs: <https://clickhouse.com/docs/en/operations/settings/settings_users/> -->

    <users>
    <default>
        <access_management>1</access_management>
        <named_collection_control>1</named_collection_control>
        <show_named_collections>1</show_named_collections>
        <show_named_collections_secrets>1</show_named_collections_secrets>
        <ip>::1</ip>
        <ip>127.0.0.1</ip>
    </default>
    </users>
</clickhouse>
```

Ensuite, il faut créer un user "admin" comme précisé dans la doc de clickhouse : https://clickhouse.com/docs/en/operations/access-rights#access-control-usage.
Puis supprimer le user `default` dans le fichier précdemment créé.

Pour la configuration serveur, il faut créer un fichier `config.d/config.xml` basé sur ce template :

```xml
<clickhouse>
    <http_port>8123</http_port>  <!-- En fonction des valeurs dans le .env -->
    <tcp_port>9000</tcp_port> <!-- En fonction des valeurs dans le .env -->
    <listen_host>0.0.0.0</listen_host> <!-- N'écoute pas les connexions extérieures. -->
</clickhouse>
```

## Déploiement

Pour déployer le conteneur ClickHouse, exécutez la commande suivante dans le répertoire actuel :

```bash
docker-compose up -d
```

Cela va démarrer le service ClickHouse en arrière-plan.

### Accès au service

Vous pouvez accéder à l'interface web de ClickHouse via l'URL suivante :

http://localhost:8123/play?user=admin&password=votre_mot_de_passe
Remplacez votre_mot_de_passe par le mot de passe que vous avez choisi pour l'utilisateur "admin".

## Arrêt du service

Pour arrêter le service ClickHouse, exécutez la commande suivante :

```bash
docker-compose down
```

Cela va arrêter les conteneurs Docker associés à ce service.

## Sauvegarde et restauration

Pour sauvegarder les données de ClickHouse, vous pouvez utiliser le conteneur en mode de sauvegarde :

```bash
docker exec -it <nom_du_conteneur> clickhouse-server --backup /var/lib/clickhouse/backup/
```

Assurez-vous que le répertoire /var/lib/clickhouse/backup/ est monté sur votre hôte pour accéder à la sauvegarde.

Pour restaurer les données, utilisez :

```bash
docker exec -it <nom_du_conteneur> clickhouse-server --restore /var/lib/clickhouse/backup/
```

N'oubliez pas de remplacer `<nom_du_conteneur>` par le nom réel de votre conteneur ClickHouse.
