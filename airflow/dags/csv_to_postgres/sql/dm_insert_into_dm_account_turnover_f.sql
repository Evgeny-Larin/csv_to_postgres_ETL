insert into dm.dm_account_turnover_f 

select *

from
(
	select 
		oper_date, mad.account_number,
		sum(trun_deb) as deb_trun_rub,
		sum(trun_deb)/1000 as deb_trun_th_rub,
		sum(trun_cred) as cre_trun_rub,
		sum(trun_cred)/1000 as cre_trun_th_rub
	from (
	
				select 
					oper_date, debet_account_rk as acc,
					sum(debet_amount) as trun_deb,
					0 as trun_cred
				from ds.ft_posting_f fpf 
				group by oper_date, debet_account_rk
				
				union all
				
				select 
					oper_date, credit_account_rk as acc, 
					0 as trun_deb,
					sum(credit_amount) as trun_cred
				from ds.ft_posting_f fpf 
				group by oper_date, credit_account_rk
				
	) as cred_deb
	
	join ds.md_account_d mad 
	on cred_deb.acc = mad.account_rk 
	group by oper_date, mad.account_number
	order by oper_date
) as result_table

on conflict (on_date, acct_num) 
do update
set 
	deb_trun_rub=EXCLUDED.deb_trun_rub,
	deb_trun_th_rub=EXCLUDED.deb_trun_th_rub,
	cre_trun_rub=EXCLUDED.cre_trun_rub,
	cre_trun_th_rub=EXCLUDED.cre_trun_th_rub