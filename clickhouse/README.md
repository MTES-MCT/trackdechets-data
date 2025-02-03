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

Ensuite, il faut créer un user "admin" comme prcis dans la doc de clickhouse : https://clickhouse.com/docs/en/operations/access-rights#access-control-usage.
Puis supprimer le user `default` dans le fichier précdemment créé.
