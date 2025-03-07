with source as (
        select * from {{ source('raw_zone', 'isdd_capacites_limites_50pct') }}
  ),
  renamed as (
      select
        {{ adapter.quote("SIRET") }} as siret,
        {{ adapter.quote("RÉGION") }} as region,
        {{ adapter.quote("DÉPARTEMENT") }} as departement,
        {{ adapter.quote("NOM SITE") }} as nom_site,
        {{ adapter.quote("COMMUNE") }} as commune,
        {{ adapter.quote("CAPACITÉ AUTORISÉE 50") }} as capacite_50pct,
        {{ adapter.quote("INSCRIT") }} as inscrit,
        {{ adapter.quote("PROFIL ISDND") }} as profil_isdnd,
        {{ adapter.quote("PROFIL MAJ") }} as profil_maj,
        {{ adapter.quote("INSCRIT RNDTS") }} as inscrit_rndts,
        {{ adapter.quote("commentaires") }}

      from source
  )
  select * from renamed
    