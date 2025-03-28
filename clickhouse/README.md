# Trackdéchets ClickHouse

## Initialisation

Il faut créer un fichier `users.d/default-user.xml` contenant :

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
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <listen_host>0.0.0.0</listen_host>
</clickhouse>
```

Il faut créer un réseau docker comme suit :

```bash
docker network create data_external_network
```
