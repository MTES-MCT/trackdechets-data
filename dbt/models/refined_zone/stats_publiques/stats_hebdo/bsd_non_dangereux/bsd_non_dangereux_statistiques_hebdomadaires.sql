SELECT *
FROM
    {{ ref('bsd_non_dangereux_created_by_week') }}
FULL OUTER JOIN {{ ref('bsd_non_dangereux_emitted_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsd_non_dangereux_sent_by_week') }} USING (semaine)
FULL OUTER JOIN
    {{ ref('bsd_non_dangereux_received_by_week') }}
    USING (semaine)
FULL OUTER JOIN
    {{ ref('bsd_non_dangereux_processed_by_week') }}
    USING (semaine)
ORDER BY semaine DESC
