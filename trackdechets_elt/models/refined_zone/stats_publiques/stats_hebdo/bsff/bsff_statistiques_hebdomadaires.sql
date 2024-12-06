SELECT *
FROM
    {{ ref('bsff_created_by_week') }}
FULL OUTER JOIN {{ ref('bsff_emitted_by_week') }} USING ("semaine")
FULL OUTER JOIN {{ ref('bsff_sent_by_week') }} USING ("semaine")
FULL OUTER JOIN {{ ref('bsff_received_by_week') }} USING ("semaine")
FULL OUTER JOIN {{ ref('bsff_processed_by_week') }} USING ("semaine")
ORDER BY semaine DESC
