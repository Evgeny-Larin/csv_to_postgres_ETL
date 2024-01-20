-- вставляем в таблицу dm_account_turnover_f, при конфликтах do update
insert into dm.dm_account_turnover_f 

select *

from
(

-- считаем сумму в рублях для иностранной валюты
	select 
		oper_date, acc.account_number,
		round(sum(case when curr.code_iso_char = 'RUB' then trun_deb else trun_deb * exrate.reduced_cource end),2) as deb_trun_rub,
		round(sum(case when curr.code_iso_char = 'RUB' then trun_deb else trun_deb * exrate.reduced_cource end)/1000,4) as deb_trun_th_rub,
		round(sum(case when curr.code_iso_char = 'RUB' then trun_cred else trun_cred * exrate.reduced_cource end),2) as cre_trun_rub,
		round(sum(case when curr.code_iso_char = 'RUB' then trun_cred else trun_cred * exrate.reduced_cource end)/1000,4) as cre_trun_th_rub
	from (
	
				-- получаем обороты по дебету и кредиту за день, предыдущий дню выполнения дага
				select 
					oper_date, debet_account_rk as account_rk,
					sum(debet_amount) as trun_deb,
					0 as trun_cred
				from ds.ft_posting_f fpf
				where oper_date = '{{ yesterday_ds  }}'
				group by oper_date, debet_account_rk
				
				union all
				
				select 
					oper_date, credit_account_rk as account_rk, 
					0 as trun_deb,
					sum(credit_amount) as trun_cred
				from ds.ft_posting_f fpf 
				where oper_date = '{{ yesterday_ds  }}'
				group by oper_date, credit_account_rk
				
	) as cred_deb
	
	join ds.md_account_d acc 
	on cred_deb.account_rk = acc.account_rk 
	join ds.md_currency_d curr
	on acc.currency_rk = curr.currency_rk 
	left join ds.md_exchange_rate_d exrate
	on acc.currency_rk = exrate.currency_rk and oper_date between exrate.data_actual_date and exrate.data_actual_end_date
	group by oper_date, acc.account_number
	order by oper_date
) as result_table

on conflict (on_date, acct_num) 
do update
set 
	deb_trun_rub=EXCLUDED.deb_trun_rub,
	deb_trun_th_rub=EXCLUDED.deb_trun_th_rub,
	cre_trun_rub=EXCLUDED.cre_trun_rub,
	cre_trun_th_rub=EXCLUDED.cre_trun_th_rub