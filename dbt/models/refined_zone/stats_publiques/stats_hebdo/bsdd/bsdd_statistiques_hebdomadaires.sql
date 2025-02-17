SELECT *
FROM
    {{ ref('bsdd_created_by_week') }}
FULL OUTER JOIN {{ ref('bsdd_emitted_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsdd_sent_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsdd_received_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsdd_processed_by_week') }} USING (semaine)
ORDER BY semaine DESC
