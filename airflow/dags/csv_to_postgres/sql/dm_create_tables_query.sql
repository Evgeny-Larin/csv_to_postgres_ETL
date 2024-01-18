-- создаём схему
create schema if not exists "dm";

-- создаём таблицы 
-- таблица оборотов
create table if not exists 
	dm.dm_account_turnover_f (
		on_date date not null,
		acct_num varchar(63) not null,
		deb_trun_rub numeric,
		deb_trun_th_rub numeric(33,4),
		cre_trun_rub numeric,
		cre_trun_th_rub numeric(33,4),
		PRIMARY KEY(on_date, acct_num)
);

-- таблица 101-й отчётной формы
create table if not exists 
	dm.dm_f101_round_f (
		regn numeric(4),
		plan varchar(1),
		num_sc varchar(5),
		a_p varchar(1),
		vr numeric(16),
		vv numeric(16),
		vitg numeric(33,4),
		ora numeric(16),
		ova numeric(16),
		oitga numeric(33,4),
		orp numeric(16),
		ovp numeric(16),
		oitgp numeric(33,4),
		ir numeric(16),
		iv numeric(16),
		iitg numeric(33,4),
		dt date,
		priz numeric(1),
		PRIMARY KEY(regn, num_sc, dt)
);
