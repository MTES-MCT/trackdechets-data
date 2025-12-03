COMPANIES_GEOCODED_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_referentials.companies_geocoded_by_ban_tmp (
	siret Nullable(String),
	adresse Nullable(String),
	code_commune_insee Nullable(String),
	longitude Nullable(String),
	latitude Nullable(String),
	result_score Nullable(String),
	result_score_next Nullable(String),
	result_label Nullable(String),
	result_type Nullable(String),
	result_id Nullable(String),
	result_housenumber Nullable(String),
	result_name Nullable(String),
	result_street Nullable(String),
	result_postcode Nullable(String),
	result_city Nullable(String),
	result_context Nullable(String),
	result_citycode Nullable(String),
	result_oldcitycode Nullable(String),
	result_oldcity Nullable(String),
	result_district Nullable(String),
	result_status Nullable(String)
)
ENGINE = MergeTree()
ORDER BY ()
"""
