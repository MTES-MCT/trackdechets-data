SELECT *
FROM
    {{ ref('bsda_created_by_week') }}
FULL OUTER JOIN {{ ref('bsda_emitted_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsda_sent_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsda_received_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsda_processed_by_week') }} USING (semaine)
ORDER BY semaine DESC
