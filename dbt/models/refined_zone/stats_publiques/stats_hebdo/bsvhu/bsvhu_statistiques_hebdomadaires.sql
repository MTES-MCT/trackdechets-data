SELECT *
FROM
    {{ ref('bsvhu_created_by_week') }}
FULL OUTER JOIN {{ ref('bsvhu_emitted_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsvhu_sent_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsvhu_received_by_week') }} USING (semaine)
FULL OUTER JOIN {{ ref('bsvhu_processed_by_week') }} USING (semaine)
ORDER BY semaine DESC
