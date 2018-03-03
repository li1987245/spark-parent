hive --hiveconf hive.root.logger=WARN,console
beeline -u "jdbc:hive2://credit02:2181,credit06:2181,credit05:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hdfs -p hdfs
select * from platform_data.cust_info limit 1;

<!--用户基础信息-->
select ACCS_NBR_MD5,CUST_SERVICE_LEVEL,MEMBERSHIP_LEVEL,GENDER,CUST_STATE_NAME,AGE
,CREDIT_LEVEL,OPEN_DATE,UNINSTALL_DATE,ONLINE_DUR,CUST_POINT,ADD_POINT,PAYMENT_MODE_NAME
from INTEG_USER_INFO_MASK_M
<!--用户账务信息-->
select accs_nbr_md5,month_id,pay_charge,pay_times,max_pay_method,bill_amt,late_amt from integ_user_acct_mask_m limit 1;
<!--号码状态变化-->
select ACCS_NBR_MD5,CHGCARD_TIMES,PREUNINSTALL_TIMES,INITSTOP_TIMES,LAST_PASSSTOP1_DATE
,LAST_N_PASSSTOP1_DATE,PASSSTOP1_TIMES,LAST_PASSSTOP2_DATE,LAST_N_PASSSTOP2_DATE
,PASSSTOP2_TIMES from INTEG_USER_STATECHG_MASK_D
<!--移动语音-->
select MSISDN,CALL_TYPE,CFEE,LFEE,BILLING_MODE,CALL_DURATION,LONG_TYPE,START_TIME
,END_TIME from phone_voice_detail
<!--移动上网数据-->
select MSISDN,CALL_TYPE,BASIC_FEE,FEE_ADD,BILLING_MODE,START_TIME,END_TIME
from phone_data_detail


select integ_user_info_mask_m.accs_nbr_md5,cust_service_level,membership_level,gender
,cust_state_name,age,credit_level,online_dur,cust_point,add_point,payment_mode_name
,integ_user_info_mask_m.month_id,pay_charge,pay_times,max_pay_method,bill_amt,late_amt
,phone_voice_call_type,phone_voice_month
from integ_user_info_mask_m left outer join integ_user_acct_mask_m on
integ_user_info_mask_m.accs_nbr_md5 = integ_user_acct_mask_m.accs_nbr_md5
and integ_user_info_mask_m.month_id = integ_user_acct_mask_m.month_id
left outer join (select md5(t.msisdn) as ACCS_NBR_MD5,t.call_type as phone_voice_call_type,substr(regexp_replace(start_time, '-', ''),0,6) as phone_voice_month
,sum(t.cfee) as phone_voice_cfee,sum(t.lfee) as phone_voice_lfee,sum(t.call_duration) as phone_voice_month_call_duration
from phone_voice_detail as t  group by t.msisdn,t.call_type,substr(regexp_replace(start_time, '-', ''),0,6)) a on
integ_user_info_mask_m.ACCS_NBR_MD5 = a.ACCS_NBR_MD5 and integ_user_info_mask_m.month_id = a.phone_voice_month;


set hive.execution.engine=tez;
set tez.queue.name=demo;
insert overwrite local directory '/tmp/train.csv' row format delimited fields terminated by ',' select * from (select integ_user_info_mask_m.accs_nbr_md5,cust_service_level,membership_level,gender,cust_state_name,age,credit_level,online_dur,cust_point,add_point,payment_mode_name,integ_user_info_mask_m.month_id,pay_charge,pay_times,max_pay_method,bill_amt,late_amt,phone_voice_cfee, phone_voice_lfee,phone_voice_month_call_duration from integ_user_info_mask_m left outer join integ_user_acct_mask_m on integ_user_info_mask_m.accs_nbr_md5 = integ_user_acct_mask_m.accs_nbr_md5 and integ_user_info_mask_m.month_id = integ_user_acct_mask_m.month_id left outer join (select upper(md5(t.msisdn)) as ACCS_NBR_MD5,substr(regexp_replace(start_time, '-', ''),0,6) as phone_voice_month,sum(t.cfee) as phone_voice_cfee,sum(t.lfee) as phone_voice_lfee,sum(t.call_duration) as phone_voice_month_call_duration from phone_voice_detail as t  group by t.msisdn,substr(regexp_replace(start_time, '-', ''),0,6)) a on integ_user_info_mask_m.ACCS_NBR_MD5 = a.ACCS_NBR_MD5 and integ_user_info_mask_m.month_id = a. phone_voice_month) as b sort by rand() limit 20000;