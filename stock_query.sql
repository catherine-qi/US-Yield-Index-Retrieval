create table US_High_Yield_Index
(
	value_date date,
	value dec(5,3),
	data_type char(80),
	primary key (value_date, data_type)
)

drop table US_High_Yield_Index

select *
	from US_High_Yield_Index