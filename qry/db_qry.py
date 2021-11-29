NUDGEADMIN_SERVICE_ID_1 = """
select 
   concat('''',service_num,'''') as service_num
from 
   nudge_v532.seg_stb ss 
where ss.seg_id in (
   select 
      seg_id 
   from nudge_v532.seg s 
   where 
      use_yn = 'Y' and del_yn = 'N'
      and target_type  = '2'
)
"""
GET_STB_ID = """
SELECT
    USER_SERVICE_NUM,
    STB_ID
FROM HANAROCMS.STB
WHERE SERVICE_STATUS_CODE = 1 
AND USER_SERVICE_NUM IN (%s)
AND STB_ID IS NOT NULL
"""
UPDATE_STB_ID = """
update nudge_v532.seg_stb
set
stb_id = %(STB_ID)s,
apply_dt = date_format(adddate(now(), 0), '%%Y.%%m.%%d')
where
service_num = %(USER_SERVICE_NUM)s
"""