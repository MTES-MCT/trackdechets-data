SELECT *
FROM
    {{ ref('bsdasri_created_by_week') }}
FULL OUTER JOIN {{ ref('bsdasri_emitted_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsdasri_sent_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsdasri_received_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsdasri_processed_by_week') }} USING (semaine)
ORDER BY semaine DESC
