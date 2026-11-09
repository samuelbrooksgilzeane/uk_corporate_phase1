-- A snapshot marked complete must physically have all of its archive parts.
select
    m.snapshot_date,
    m.expected_parts,
    count(distinct b.part_number) as actual_parts
from {{ source('companies_house', 'psc_bulk_manifest') }} m
left join {{ source('companies_house', 'psc_bulk_snapshots') }} b
    on m.snapshot_date = b.snapshot_date
where m.is_complete = true
group by m.snapshot_date, m.expected_parts
having count(distinct b.part_number) <> m.expected_parts

