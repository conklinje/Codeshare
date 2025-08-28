with dnc_list as (
    select '('||areacode||') '||left(phonenumber,3)||'-'||right(phonenumber,4) as formatted_phone 
    from reference.mdm.national_dnc_list
),
stage as (
    with merged as (
    with sort_contacts_zi as (
        select distinct null as identifier,
                zi_c_location_id::varchar as zi_c_location_id,
                job_title,
                first_name|| ' ' ||last_name as name,
                email_address,
                management_level,
                sandbox.conklin.normalize_phone(direct_phone_number) as direct_phone_number,
                sandbox.conklin.normalize_phone(mobile_phone) as mobile_phone,
                case
                    when email_address is not null or direct_phone_number is not null or mobile_phone is not null
                    then true
                    else false
                end as has_contact_info,
                case
                    when management_level = 'C-Level'
                    then 1
                    when management_level = 'VP-Level'
                    then 2
                    when management_level = 'Director'
                    then 3
                    when management_level = 'Manager'
                    then 4
                    when management_level = 'Non-Manager'
                    then 5
                    else 99
                end as management_level_preference,
                contact_accuracy_score,
                dense_rank() over (
                    partition by zi_c_location_id
                    order by
                        has_contact_info desc,
                        management_level_preference asc,
                        job_title_hierarchy_level desc,
                        contact_accuracy_score desc,
                        zoominfo_contact_id desc
                ) as preference_hier,
                'zi' as source,
    
            from zoominfo.brick.zi_company_contact_combined
            where has_contact_info = true
            qualify
                dense_rank() over (
                    partition by zi_c_location_id
                    order by
                        has_contact_info desc,
                        management_level_preference asc,
                        contact_accuracy_score desc,
                        zoominfo_contact_id desc
                )
                <= 10
        )
    
    
    
    
    , sort_contacts_mdm as (
        with contact_rank as (
            Select distinct identifier
                ,zi_c_location_id
                ,'Owner' as job_title
                ,case when owner_name = 'NA' then null else owner_name end as name
                ,case when owner_email = 'NA' then null else owner_email end as email_address
                ,null as management_level
                ,null as direct_phone_number
                ,case when owner_phone = 'NA' then null else sandbox.conklin.normalize_phone(owner_phone) end as mobile_phone
                ,case when owner_email <> 'NA' or owner_phone <> 'NA'
                        then true
                        else false
                end as has_contact_info
                ,null as management_level_preference
                ,null as contact_accuracy_score
                ,1 as preference_hier
                ,'mdm' as source
            from bi.mdm.customer 
            where status in ('Installed')
                and id_type <> 'CLIENT_ID'
                and owner_name <> 'NA'
            union all
        
            Select identifier
                ,zi_c_location_id
                ,'Primary Contact' as job_title
                ,case when primary_contact_name = 'NA' then null else primary_contact_name end as name
                ,case when primary_contact_email = 'NA' then null else primary_contact_email end as email_address
                ,null as management_level
                ,case when primary_contact_wrkphone = 'NA' then null else sandbox.conklin.normalize_phone(primary_contact_wrkphone) end as direct_phone_number
                ,case when primary_contact_cellphone = 'NA' then null else sandbox.conklin.normalize_phone(primary_contact_cellphone) end as mobile_phone
                ,case when primary_contact_email <> 'NA' or primary_contact_cellphone <> 'NA' or primary_contact_wrkphone <> 'NA'
                        then true
                        else false
                end as has_contact_info
                ,null as management_level_preference
                ,null as contact_accuracy_score
                ,case when owner_name <> 'NA' and primary_contact_name <> 'NA' then 2
                    else 1
                end as preference_hier
                ,'mdm' as source
            from bi.mdm.customer
            where status in ('Installed')
                and id_type <> 'CLIENT_ID'
                and primary_contact_name <> 'NA'
            )
        select * from contact_rank
    )
    
    
    select * from sort_contacts_zi
    union all
    select * from sort_contacts_mdm
    
    )
select
distinct identifier,
            zi_c_location_id,
            case when identifier is null then zi_c_location_id::varchar else identifier::varchar end as prospect_id,
            job_title,
            name,
            email_address,
            management_level,
            direct_phone_number,
            mobile_phone,
            has_contact_info,
            management_level_preference,
            contact_accuracy_score,
            preference_hier,
            dense_rank() over (
                partition by prospect_id
                order by
                    has_contact_info desc,
                    source desc,
                    management_level_preference asc,
                    contact_accuracy_score desc
            ) as preference_hier_final
from merged

)



select identifier,
       zi_c_location_id,
       prospect_id,
       object_agg(contact_rank,
           object_construct_keep_null(
               'name', name,
               'email_address', email_address,
               'management_level', management_level,
               'job_title', job_title,
               'direct_phone_number', direct_phone_number,
               'mobile_phone', mobile_phone,
               'contact_accuracy_score', contact_accuracy_score,
               'is_dnc', case when dnc_direct is not null or dnc_mobile is not null then true else false end
           )
       ) as top10_contacts
from (
    select *,
           row_number() over (
               partition by prospect_id
               order by preference_hier_final asc
           ) as contact_rank
    from (
        select s.*, dnc1.formatted_phone as dnc_direct, dnc2.formatted_phone as dnc_mobile
        from stage s
        left join dnc_list dnc1 on s.direct_phone_number = dnc1.formatted_phone
        left join dnc_list dnc2 on s.mobile_phone = dnc2.formatted_phone
    )
    where preference_hier_final <= 10
) t
group by all