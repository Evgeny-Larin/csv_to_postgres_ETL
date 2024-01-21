begin transaction;

create or replace view dm.vw_turn_deb_cre
as (

-- получаем для каждого счёта обороты по дебету и кредиту за отчётную дату
with turns as 
(
select 
	pst.oper_date,
	acc.account_number,
	acc.currency_rk, 
	sum(pst.debet_amount) as turn_deb,
	0 as turn_cred
from ds.ft_posting_f pst 
join ds.md_account_d acc
on pst.debet_account_rk = acc.account_rk 
where pst.oper_date = '{{  yesterday_ds  }}'
group by pst.oper_date, acc.account_number, acc.currency_rk


union all

select 
	pst.oper_date,
	acc.account_number,
	acc.currency_rk, 
	0 as turn_deb,
	sum(pst.credit_amount) as turn_cred
from ds.ft_posting_f pst 
join ds.md_account_d acc
on pst.credit_account_rk = acc.account_rk 
where pst.oper_date = '{{  yesterday_ds  }}'
group by pst.oper_date, acc.account_number, acc.currency_rk
)


-- подтягиваем курс иностранных валют, рассчитываем ora, ova, orp, ovp

select 
	turns.oper_date,
	account_number,
	case when curr.code_iso_char = 'RUB' then turns.turn_deb/1000 else 0 end as ora,
	case when curr.code_iso_char <> 'RUB' then turns.turn_deb * exrate.reduced_cource/1000 else 0 end as ova,
	case when curr.code_iso_char = 'RUB' then turns.turn_cred/1000 else 0 end as orp,
	case when curr.code_iso_char <> 'RUB' then turns.turn_cred * exrate.reduced_cource/1000 else 0 end as ovp
from turns
join ds.md_currency_d curr
on turns.currency_rk = curr.currency_rk 
left join ds.md_exchange_rate_d exrate
on turns.currency_rk = exrate.currency_rk  and turns.oper_date between exrate.data_actual_date and exrate.data_actual_end_date
)
;

create or replace view dm.vw_in_bal
as (

-- получаем входящие остатки
select
	bal.on_date as dt,
	ledg.chapter as plan,
	acc.account_number as num_sc,
	case when acc.char_type = 'A' then '1' when acc.char_type = 'P' then '2' end as a_p,
	case when curr.code_iso_char = 'RUB' then bal.balance_out/1000 else 0 end as vr,
	case when curr.code_iso_char <> 'RUB' then bal.balance_out * exrate.reduced_cource/1000 else 0 end as vv
from ds.md_account_d acc 
join ds.md_ledger_account_s ledg 
ON substring(acc.account_number, 1, 5)::int = ledg.ledger_account
join ds.ft_balance_f bal
on acc.account_rk = bal.account_rk 
join ds.md_currency_d curr
on acc.currency_rk = curr.currency_rk 
left join ds.md_exchange_rate_d exrate
on acc.currency_rk = exrate.currency_rk and bal.on_date between exrate.data_actual_date and exrate.data_actual_end_date
where bal.on_date = (
					-- получаем дату, предыдущую дню выполнения дага, или наиближаюйшую прошлую дату
					 select max(bal.on_date)
					 from ds.ft_balance_f bal 
					 where bal.on_date <= '{{  yesterday_ds  }}'
)
)
;

create or replace view dm.vw_in_bal_and_turns
as (

-- считаем столбцы входящих остатков и оборотов для каждого счёта
select 
	in_bal.plan,
	in_bal.num_sc,
	in_bal.a_p,
	coalesce(sum(vr),0) as vr,
	coalesce(sum(vv),0) as vv,
	coalesce(sum(vr + vv),0) as vitg,
	coalesce(sum(ora),0) as ora,
	coalesce(sum(ova),0) as ova,
	coalesce(sum(ora + ova),0) as oitga,
	coalesce(sum(orp),0) as orp,
	coalesce(sum(ovp),0) as ovp,
	coalesce(sum(orp + ovp),0) as oitgp
from dm.vw_in_bal as in_bal
left join dm.vw_turn_deb_cre as turn_deb_cre
on in_bal.num_sc = turn_deb_cre.account_number
group by turn_deb_cre.oper_date, in_bal.plan, in_bal.num_sc, in_bal.a_p
)
;

create or replace view vw_out_bal as (

-- считаем исходящие остатки в рублях и валюте для каждого счёта
select
	plan,
    num_sc, 
    a_p, 
    vr, 
    vv, 
    vitg, 
    ora, 
    ova, 
    oitga, 
    orp, 
    ovp, 
    oitgp,
	case 
		when a_p = '1' and (acc.currency_code = '643' or acc.currency_code = '810') 
		then vr - orp + ora 
		when a_p = '2' and (acc.currency_code = '643' or acc.currency_code = '810')
		then vr + orp - ora 
		else 0
	end as ir,
	case 
		when a_p = '1' and (acc.currency_code <> '643' and acc.currency_code <> '810') 
		then vv - ovp + ova 
		when a_p = '2' and (acc.currency_code <> '643' or acc.currency_code <> '810')
		then vv + ovp - ova
		else 0
	end as iv
from dm.vw_in_bal_and_turns vibat
left join ds.md_account_d acc
on vibat.num_sc = acc.account_number
);

create or replace view dm.vw_f101_result as (


-- группируем все показатели для счетов второго порядка, считаем исходящие остатки "итого", добавляем арибуты
select 
       1481 as regn,
       plan::char(1),
       substring(num_sc, 1, 5)::char(5) as num_sc,
       a_p::char(1), 
	   sum(vr)::numeric(16) as vr,
	   sum(vv)::numeric(16) as vv, 
	   sum(vitg)::numeric(33,4) as vitg, 
	   sum(ora)::numeric(16) as ora, 
	   sum(ova)::numeric(16) as ova, 
	   sum(oitga)::numeric(33,4) as oitga, 
	   sum(orp)::numeric(16) as orp, 
	   sum(ovp)::numeric(16) as ovp, 
	   sum(oitgp)::numeric(33,4) as oitgp, 
	   sum(ir)::numeric(16) as ir, 
	   sum(iv)::numeric(16) as iv,
	   sum(ir+iv)::numeric(33,4) as iitg,
	   '{{  yesterday_ds  }}'::date as dt,
       1 as priz
from vw_out_bal
group by dt, priz, regn, plan, a_p, substring(num_sc, 1, 5)
)
;


-- записываем в настоящую таблицу, on conflict do update
insert into dm.dm_f101_round_f
select *
from dm.vw_f101_result
on conflict (regn, num_sc, dt) do update
set 
	plan=EXCLUDED.plan, 
	a_p=EXCLUDED.a_p, 
	vr=EXCLUDED.vr, 
	vv=EXCLUDED.vv, 
	vitg=EXCLUDED.vitg, 
	ora=EXCLUDED.ora, 
	ova=EXCLUDED.ova, 
	oitga=EXCLUDED.oitga, 
	orp=EXCLUDED.orp, 
	ovp=EXCLUDED.ovp, 
	oitgp=EXCLUDED.oitgp, 
	ir=EXCLUDED.ir, 
	iv=EXCLUDED.iv, 
	iitg=EXCLUDED.iitg, 
	priz=EXCLUDED.priz
;


-- записываем в таблицу ft_balance_f исходящий баланс на дату для каждого счёта
insert into ds.ft_balance_f

select 
	'{{  ds  }}' as on_date,
	acc.account_rk as account_rk,
	acc.currency_rk as currency_rk,
	case when curr.code_iso_char = 'RUB' then ir*1000 else iv*1000/exrate.reduced_cource end as balance_out
from vw_out_bal 
join ds.md_account_d acc 
on vw_out_bal.num_sc = acc.account_number 
left join ds.md_currency_d curr
on acc.currency_rk = curr.currency_rk
join ds.md_exchange_rate_d exrate
on acc.currency_rk = exrate.currency_rk and '{{  ds  }}' between exrate.data_actual_date and exrate.data_actual_end_date 

on conflict (on_date, account_rk) do update
set 
	currency_rk=EXCLUDED.currency_rk, 
	balance_out=EXCLUDED.balance_out
;

drop view dm.vw_f101_result, dm.vw_in_bal, dm.vw_in_bal_and_turns, dm.vw_turn_deb_cre, vw_out_bal cascade;


commit;


