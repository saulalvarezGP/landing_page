WITH 

ticket AS (
    WITH 
    assignee AS (
        SELECT DISTINCT
            tfh.ticket_id::int,
            date_trunc('second',tfh.updated::TIMESTAMP) AS time_utc,
            CASE WHEN g.id IS NOT NULL THEN CONCAT('02_Assignee Change GROUP', '|', g.name) END AS event,
            CASE WHEN usr.id IS NOT NULL THEN CONCAT('02_Assignee Change USER', '|', usr.name) END AS assignee_user
        FROM zendesk.ticket_field_history tfh
        LEFT JOIN zendesk.user usr ON tfh.value::numeric=usr.id::numeric
        LEFT JOIN zendesk.time_zone as tz ON usr.time_zone = tz.time_zone
        LEFT JOIN zendesk.group as g ON tfh.value::NUMERIC = g.id
        WHERE  
            field_name IN ('assignee_id', 'group_id')
            AND tfh.ticket_id IN( 193642)--, 207230, 217805)
        ORDER by 1,  date_trunc('second',tfh.updated::TIMESTAMP)
    )
    ,comm AS (
        SELECT  
            tm.ticket_id::int,
            date_trunc('second',tm.created::TIMESTAMP) AS time_utc,
            CONCAT('04_Comment',' ',usr.role, ' - ',usr.name, ' - ', CASE WHEN tm.public = TRUE THEN 'Public' ELSE 'Not Public' END ) AS event,
            '' AS assignee_user
        FROM zendesk.ticket_comment AS tm
        LEFT JOIN  zendesk.user AS usr on tm.user_id=usr.id
        LEFT JOIN zendesk.time_zone AS tz ON usr.time_zone = tz.time_zone
        WHERE ticket_id IN( 193642)--, 207230, 217805)
        ORDER BY tm.ticket_id, tm.created
    )
    ,sla AS (
        SELECT 
            replace(ticket_id,'.0','')::int,
            date_trunc('second',time::TIMESTAMP) AS time_utc,
            case 
                when type='apply_sla' then CONCAT ('03_',type, ' - ', metric)
                else CONCAT ('05_',type, ' - ', metric)
            end AS event,
            '' AS assignee_user
        FROM zendesk.ticket_metric_events
        WHERE true
            and ticket_id IN( '193642')--, '207230', '217805')
            AND type IN ('apply_sla', 'breach', 'fulfill')
            AND sla <> 'None'
        ORDER BY ticket_id::BIGINT,time
    )
    ,created AS (
        SELECT 
            id::int,
			date_trunc('second',created_at::TIMESTAMP) AS time_utc,
			'01_Ticket Created' AS event,
			'' AS assignee_user
        FROM zendesk.ticket
        --WHERE id IN( 193642)--, 207230, 217805)
        ORDER BY id,created_at		
    )

    SELECT * FROM assignee
    UNION
    SELECT * FROM comm
    UNION
    SELECT * FROM sla
    UNION
    SELECT * FROM created
    ORDER BY ticket_id, time_utc
)

,prefilled as (
    select 
        a.ticket_id
        ,a.time_utc
        ,a.event
        ,split_part(c.assignee_group,'|',2) as assignee_group_per_ticketsecond
        ,split_part(b.assignee_user,'|',2) as assignee_user_per_ticketsecond
    from ticket a
    left join (
        select distinct 
            ticket_id
            ,time_utc
            ,assignee_user 
        from ticket 
        where assignee_user is not null and assignee_user<>''
        ) b on a.ticket_id=b.ticket_id and a.time_utc=b.time_utc
    left join (
        select distinct 
            ticket_id
            ,time_utc
            ,event as assignee_group
        from ticket 
        where event like '%GROUP%'
        ) c on a.ticket_id=c.ticket_id and a.time_utc=c.time_utc
    where event is not null
    ORDER BY ticket_id, time_utc, event
)

--- para python
select * from prefilled