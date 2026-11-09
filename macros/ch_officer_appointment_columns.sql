{% macro ch_officer_appointment_columns() %}
    coalesce(
        nullif(regexp_extract(resource_uri, '^/company/([^/]+)', 1), ''),
        nullif(regexp_extract(get_json_object(data_json, '$.links.self'), '^/company/([^/]+)', 1), '')
    ) as company_number,
    nullif(regexp_extract(
        get_json_object(data_json, '$.links.officer.appointments'),
        '/officers/([^/]+)/appointments', 1
    ), '') as officer_id,
    get_json_object(data_json, '$.name') as officer_name,
    get_json_object(data_json, '$.officer_role') as officer_role,
    to_date(get_json_object(data_json, '$.appointed_on')) as appointed_on,
    to_date(get_json_object(data_json, '$.resigned_on')) as resigned_on,
    cast(get_json_object(data_json, '$.date_of_birth.year') as int) as birth_year,
    cast(get_json_object(data_json, '$.date_of_birth.month') as int) as birth_month
{% endmacro %}
