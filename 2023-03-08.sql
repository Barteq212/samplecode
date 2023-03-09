
drop table if exists #transaction_table

select top 1000000
row_number() over (order by (select null)) [TRANSACTION_ID],
abs(checksum(newid()))%1000 [PRODUCT_ID],
abs(checksum(newid()))%100 [USSER_ID],
abs(checksum(newid()))%100.345 [SPEND],
dateadd(dd,abs(checksum(newid()))%1000,DATEADD(ss, abs(checksum(newid()))%1000, '2006-08-31')) [TRANSACTION_DATE]
into #transaction_table
from sys.columns, sys.columns c


select * from (
select 
distinct
tt.USSER_ID,
(select top 1 td.SPEND from #transaction_table td where td.USSER_ID=tt.USSER_ID order by td.TRANSACTION_DATE) trans_value
from #transaction_table tt
)bb
where bb.trans_value>50
order by USSER_ID

select * from (
select tt.USSER_ID,tt.SPEND,rank() over (partition by USSER_ID order by transaction_date asc) [rankk]   from #transaction_table tt) c
where c.rankk=1 and c.SPEND>50
order by USSER_ID

select distinct tt.USSER_ID,tz.SPEND from #transaction_table tt
outer apply (select top 1 td.SPEND from #transaction_table td where tt.USSER_ID=td.USSER_ID order by td.TRANSACTION_DATE) tz
where tz.SPEND>50
order by USSER_ID



select * from #transaction_table tp
join (

select tt.USSER_ID,min(tt.TRANSACTION_DATE) [min_date] from #transaction_table tt
group by tt.USSER_ID) dd on tp.USSER_ID=dd.USSER_ID and dd.min_date=tp.TRANSACTION_DATE
where tp.SPEND>50
order by tp.USSER_ID
