
-- Script to run in Sandbox or other Non-Prod environment where we don't want to use Production Zammad data (for RGPD compliance)
CREATE DATABASE raw_zone_zammad;

-- raw_zone_zammad.groups definition
CREATE TABLE raw_zone_zammad.groups
(   `id` Int64,`signature_id` Nullable(Int64),
    `email_address_id` Nullable(Int64),
    `name` Nullable(String),
    `follow_up_possible` Nullable(String),
    `follow_up_assignment` Nullable(Bool),
    `active` Nullable(Bool),
    `note` Nullable(String),
    `updated_by_id` Nullable(Int64),
    `created_by_id` Nullable(Int64),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `shared_drafts` Nullable(Bool),
    `name_last` Nullable(String),
    `user_ids` Nullable(String),
    `_dlt_load_id` String,
    `_dlt_id` String,
    `parent_id` Nullable(Int64),
    `value` Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;


-- raw_zone_zammad.organizations definition

CREATE TABLE raw_zone_zammad.organizations
(
`id` Int64,
`name` Nullable(String),
`shared` Nullable(Bool),
`domain` Nullable(String),
`domain_assignment` Nullable(Bool),
`active` Nullable(Bool),
`note` Nullable(String),
`updated_by_id` Nullable(Int64),
`created_by_id` Nullable(Int64),
`created_at` Nullable(DateTime64(6, 'UTC')),
`updated_at` Nullable(DateTime64(6, 'UTC')),
`vip` Nullable(Bool),
`member_ids` Nullable(String),
`secondary_member_ids` Nullable(String),
`_dlt_load_id` String,
`_dlt_id` String
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;


-- raw_zone_zammad.text_modules definition

CREATE TABLE raw_zone_zammad.text_modules
(
`id` Int64,
`name` Nullable(String),
`keywords` Nullable(String),
`content` Nullable(String),
`active` Nullable(Bool),
`updated_by_id` Nullable(Int64),
`created_by_id` Nullable(Int64),
`created_at` Nullable(DateTime64(6, 'UTC')),
`updated_at` Nullable(DateTime64(6, 'UTC')),
`group_ids` Nullable(String),
`_dlt_load_id` String,
`_dlt_id` String,
`note` Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;


-- raw_zone_zammad.tickets definition

CREATE TABLE raw_zone_zammad.tickets
(
`id` Int64,
`group_id` Nullable(Int64),
`priority_id` Nullable(Int64),
`state_id` Nullable(Int64),
`number` Nullable(String),
`title` Nullable(String),
`owner_id` Nullable(Int64),
`customer_id` Nullable(Int64),
`close_at` Nullable(DateTime64(6, 'UTC')),
`last_contact_at` Nullable(DateTime64(6, 'UTC')),
`last_contact_customer_at` Nullable(DateTime64(6, 'UTC')),
`create_article_type_id` Nullable(Int64),
`create_article_sender_id` Nullable(Int64),
`article_count` Nullable(Int64),
`preferences` Nullable(String),
`updated_by_id` Nullable(Int64),
`created_by_id` Nullable(Int64),
`created_at` Nullable(DateTime64(6, 'UTC')),
`updated_at` Nullable(DateTime64(6, 'UTC')),
`referencing_checklist_ids` Nullable(String),
`article_ids` Nullable(String),
`ticket_time_accounting_ids` Nullable(String),
`tags` Nullable(String),
`_dlt_load_id` String,
`_dlt_id` String,
`first_response_at` Nullable(DateTime64(6, 'UTC')),
`last_contact_agent_at` Nullable(DateTime64(6, 'UTC')),
`last_owner_update_at` Nullable(DateTime64(6, 'UTC')),
`time_unit` Nullable(String),
`organization_id` Nullable(Int64),
`first_response_in_min` Nullable(Int64),
`first_response_diff_in_min` Nullable(Int64),
`last_close_at` Nullable(DateTime64(6, 'UTC')),
`pending_time` Nullable(DateTime64(6, 'UTC')),
`first_response_escalation_at` Nullable(DateTime64(6, 'UTC')),
`escalation_at` Nullable(DateTime64(6, 'UTC'))
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;


-- raw_zone_zammad.users definition

CREATE TABLE raw_zone_zammad.users
(
`id` Int64,
`login` Nullable(String),
`firstname` Nullable(String),
`lastname` Nullable(String),
`email` Nullable(String),
`web` Nullable(String),
`phone` Nullable(String),
`fax` Nullable(String),
`mobile` Nullable(String),
`department` Nullable(String),
`street` Nullable(String),
`zip` Nullable(String),
`city` Nullable(String),
`country` Nullable(String),
`address` Nullable(String),
`vip` Nullable(Bool),
`verified` Nullable(Bool),
`active` Nullable(Bool),
`note` Nullable(String),
`login_failed` Nullable(Int64),
`out_of_office` Nullable(Bool),
`preferences` Nullable(String),
`updated_by_id` Nullable(Int64),
`created_by_id` Nullable(Int64),
`created_at` Nullable(DateTime64(6, 'UTC')),
`updated_at` Nullable(DateTime64(6, 'UTC')),
`role_ids` Nullable(String),
`two_factor_preference_ids` Nullable(String),
`organization_ids` Nullable(String),
`authorization_ids` Nullable(String),
`overview_sorting_ids` Nullable(String),
`group_ids` Nullable(String),
`_dlt_load_id` String,
`_dlt_id` String,
`organization_id` Nullable(Int64),
`image` Nullable(String),
`last_login` Nullable(DateTime64(6, 'UTC')),
`source` Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;