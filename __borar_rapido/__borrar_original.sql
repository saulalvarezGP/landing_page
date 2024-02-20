WITH 

ticket AS (
    WITH 
    assignee AS (
        SELECT DISTINCT
            tfh.ticket_id,
            date_trunc('second',tfh.updated::TIMESTAMP) AS time_utc,
            CASE WHEN g.id IS NOT NULL THEN CONCAT('Assignee Change', ' -', g.name) END AS event,
            CASE WHEN usr.id IS NOT NULL THEN CONCAT('Assignee Change', ' -', usr.name) END AS assignee_user
        FROM zendesk.ticket_field_history tfh
        LEFT JOIN zendesk.user usr ON tfh.value::numeric=usr.id::numeric
        LEFT JOIN zendesk.time_zone as tz ON usr.time_zone = tz.time_zone
        LEFT JOIN zendesk.group as g ON tfh.value::NUMERIC = g.id
        WHERE  
            field_name IN ('assignee_id', 'group_id')
            AND tfh.ticket_id IN( 193642)--, 207230, 217805)
        ORDER by tfh.ticket_id,  date_trunc('second',tfh.updated::TIMESTAMP)
    )
    ,comm AS (
        SELECT  
            tm.ticket_id,
            date_trunc('second',tm.created::TIMESTAMP) AS time_utc,
            CONCAT('Comment',' ',usr.role, ' - ',usr.name, ' - ', CASE WHEN tm.public = TRUE THEN 'Public' ELSE 'Not Public' END ) AS event,
            '' AS assignee_user
        FROM zendesk.ticket_comment AS tm
        LEFT JOIN  zendesk.user AS usr on tm.user_id=usr.id
        LEFT JOIN zendesk.time_zone AS tz ON usr.time_zone = tz.time_zone
        WHERE ticket_id IN( 193642)--, 207230, 217805)
        ORDER BY tm.ticket_id, tm.created
    )
    ,sla AS (
        SELECT 
            ticket_id::BIGINT,
            date_trunc('second',time::TIMESTAMP) AS time_utc,
            CONCAT (type, ' - ', metric) AS event,
            '' AS assignee_user
        FROM zendesk.ticket_metric_events
        WHERE 
            ticket_id IN( '193642')--, '207230', '217805')
            AND type IN ('apply_sla', 'breach', 'fulfill')
            AND sla <> 'None'
        ORDER BY ticket_id::BIGINT,time
    )
    ,created AS (
        SELECT 
            id::BIGINT,
			date_trunc('second',created_at::TIMESTAMP) AS time_utc,
			'Ticket Created' AS event,
			'' AS assignee_user
        FROM zendesk.ticket
        WHERE id IN( 193642)--, 207230, 217805)
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

,a as (
    SELECT 
        ticket.*,
        CASE WHEN event ILIKE ('%Assignee%')THEN event  END AS assignee_group,
	    CASE WHEN assignee_user IS NOT NULL THEN assignee_user ELSE NULL END AS assignee_user2
    FROM ticket
    ORDER BY ticket_id, time_utc
)

,b AS (
    SELECT 
        a.*,
        CONCAT(time_utc,' ',(ROW_NUMBER() OVER (ORDER BY ticket_id))) as rownum
	 FROM A
)

,b1 as (
    SELECT 
        ticket_id, 
        time_utc,assignee_group, 
        rownum
	FROM b
	WHERE assignee_group IS NOT NULL
)

,b2 as (
    SELECT 
        ticket_id
        ,time_utc,assignee_user2
        ,rownum
	FROM b
	WHERE assignee_user2 IS NOT NULL
)

,C AS (
    select 
        b.*,
        MAX(rownum) filter (where assignee_group is not null) over (order by rownum rows between unbounded preceding and 0 preceding) as dprev,
	    MAX(rownum) filter (where assignee_user2 is not null) over (order by rownum rows between unbounded preceding and 0 preceding) as dprev_user
	FROM B
	ORDER BY ticket_id, time_utc
)
	
SELECT 
    c.*, 
    split_part(b1.assignee_group,' -', 2) AS split_group,
    split_part(b2.assignee_user2,' -', 2) AS split_user
FROM C
LEFT JOIN b1 ON c.dprev = b1.rownum
LEFT JOIN b2 ON c.dprev_user = b2.rownum
ORDER By ticket_id, time_utc