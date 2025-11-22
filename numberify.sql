with expanded as (
    select distinct digi.id, (select indig.id from digimon indig where url=json_each.value) as prev_evo
    from digimon digi, json_each(prev_links)
    where prev_evo is not null
), grouped as (
    select digi.id, json_group_array(digi.prev_evo) as grp
    from expanded digi
    group by id
)
update digimon
set previous=(
    select grp from grouped where id=digimon.id
) where 1=1;
where previous is null;

with expanded as (
    select distinct digi.id, (select indig.id from digimon indig where url=json_each.value) as next_evo
    from digimon digi, json_each(next_links)
    where next_evo is not null
), grouped as (
    select digi.id, json_group_array(digi.next_evo) as grp
    from expanded digi
    group by id
)
update digimon
set next=(
    select grp from grouped where id=digimon.id
) where 1=1;
where next is null;