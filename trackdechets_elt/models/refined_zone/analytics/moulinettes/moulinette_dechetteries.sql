{{ config(
    indexes = [ {'columns': ['siret'] }]
) }}

WITH decheteries AS (

    SELECT
        c.siret,
        MAX(
            c.name
        ) AS company_name,
        MAX(
            c.contact_email
        ) AS "contact_email",
        MAX(
            c.company_types::text
        ) AS "company_types",
        MAX(
            u.email
        ) AS "email_admin"
    FROM
        {{ ref('company') }} AS c
    LEFT JOIN
        {{ ref('company_association') }}
        AS ca
        ON ca.company_id = c.id
    LEFT JOIN
        {{ ref('user') }}
        AS u
        ON ca.user_id = u.id
    WHERE
        'WASTE_CENTER' = ANY(company_types)
        AND ca."role" = 'ADMIN'
    GROUP BY
        siret
),

joined AS (
    SELECT
        decheteries.siret,
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSDD'
        ) AS "num_bsdd_destinataire",
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSDA'
        ) AS "num_bsda_destinataire",
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSFF'
        ) AS "num_bsff_destinataire",
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSDASRI'
        ) AS "num_bsdasri_destinataire",
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSVHU'
        ) AS "num_bsvhu_destinataire"
    FROM
        decheteries
    LEFT JOIN
        {{ ref('bordereaux_enriched') }}
        AS be
        ON decheteries.siret = be.destination_company_siret
    WHERE
        be.processed_at IS NOT NULL
    GROUP BY
        decheteries.siret
),

joined_2 AS (
    SELECT
        decheteries.siret,
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSDD'
        ) AS "num_bsdd_emetteur",
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSDA'
        ) AS "num_bsda_emetteur",
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSFF'
        ) AS "num_bsff_emetteur",
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSDASRI'
        ) AS "num_bsdasri_emetteur",
        COUNT(*) FILTER (
            WHERE
            be."_bs_type" = 'BSVHU'
        ) AS "num_bsvhu_emetteur"
    FROM
        decheteries
    LEFT JOIN
        {{ ref('bordereaux_enriched') }}
        AS be
        ON decheteries.siret = be.emitter_company_siret
    WHERE
        be.processed_at IS NOT NULL
    GROUP BY
        decheteries.siret
),

final_ AS (
    SELECT
        COALESCE(joined.siret, joined_2.siret)     AS siret,
        COALESCE(joined.num_bsdd_destinataire, 0)  AS num_bsdd_destinataire,
        COALESCE(joined.num_bsda_destinataire, 0)  AS num_bsda_destinataire,
        COALESCE(joined.num_bsff_destinataire, 0)  AS num_bsff_destinataire,
        COALESCE(
            joined.num_bsdasri_destinataire, 0
        )                                          AS num_bsdasri_destinataire,
        COALESCE(joined.num_bsvhu_destinataire, 0) AS num_bsvhu_destinataire,
        COALESCE(joined_2.num_bsdd_emetteur, 0)    AS num_bsdd_emetteur,
        COALESCE(joined_2.num_bsda_emetteur, 0)    AS num_bsda_emetteur,
        COALESCE(joined_2.num_bsff_emetteur, 0)    AS num_bsff_emetteur,
        COALESCE(joined_2.num_bsdasri_emetteur, 0) AS num_bsdasri_emetteur,
        COALESCE(joined_2.num_bsvhu_emetteur, 0)   AS num_bsvhu_emetteur,
        COALESCE(
            num_bsdd_destinataire,
            0
        ) + COALESCE(
            num_bsdd_emetteur,
            0
        )                                          AS "num_bsdd",
        COALESCE(
            num_bsda_destinataire,
            0
        ) + COALESCE(
            num_bsda_emetteur,
            0
        )                                          AS "num_bsda",
        COALESCE(
            num_bsff_destinataire,
            0
        ) + COALESCE(
            num_bsff_emetteur,
            0
        )                                          AS "num_bsff",
        COALESCE(
            num_bsdasri_destinataire,
            0
        ) + COALESCE(
            num_bsdasri_emetteur,
            0
        )                                          AS "num_bsdasri",
        COALESCE(
            num_bsvhu_destinataire,
            0
        ) + COALESCE(
            num_bsvhu_emetteur,
            0
        )                                          AS "num_bsvhu"
    FROM
        joined
    FULL OUTER JOIN joined_2 ON joined.siret = joined_2.siret
)

SELECT
    decheteries.siret,
    decheteries.company_name  AS "Nom de l'établissement",
    decheteries.company_types AS "Profils de l'établissement",
    decheteries.contact_email AS "E-mail de contact de l'établissement",
    decheteries."email_admin" AS "E-mail de l'admin de l'établissement",
    final_.num_bsdd_destinataire,
    final_.num_bsda_destinataire,
    final_.num_bsff_destinataire,
    final_.num_bsdasri_destinataire,
    final_.num_bsvhu_destinataire,
    final_.num_bsdd_emetteur,
    final_.num_bsda_emetteur,
    final_.num_bsff_emetteur,
    final_.num_bsdasri_emetteur,
    final_.num_bsvhu_emetteur,
    num_bsdd
    + num_bsda
    + num_bsff
    + num_bsdasri
    + num_bsvhu               AS "total_bordereaux"
FROM
    decheteries
LEFT JOIN final_ ON decheteries.siret = final_.siret
