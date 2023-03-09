
 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-ONE-PREPARE-SAMPLE-DATA----------------------------------------------
 ----------------------------------------------------------------------------------------------------------


drop table transakcje2
drop table transakcje3




select b.case_id,cast(b.amount as money) amount into transakcje2 from (
select top 1000000 ABS(checksum(newid()))%100000 [case_id],ABS(checksum(newid()))%3600.44[amount]  from sys.columns,sys.columns c)b



insert into transakcje2
select top 100000 t2.case_id,t2.amount*-1  from transakcje2 t2
order by amount desc


insert into transakcje2
select top 100000 t2.case_id,t2.amount  from transakcje2 t2
order by amount asc



insert into transakcje2
select top 100000 t2.case_id,t2.amount*1  from transakcje2 t2
order by case_id desc



insert into transakcje2
select top 100000 t2.case_id,t2.amount*-1  from transakcje2 t2
order by case_id asc


insert into transakcje2
select distinct top 100000 t2.case_id,cast(10000.05 as money) from transakcje2 t2
order by case_id desc

insert into transakcje2
select distinct top 100000 t2.case_id,cast(10000.05 as money) from transakcje2 t2
order by case_id desc

insert into transakcje2
select distinct top 100000 t2.case_id,cast(-10000.05 as money) from transakcje2 t2
order by case_id desc



select case_id,amount, ROW_NUMBER() over (partition by case_id order by amount) [transaction_id] into transakcje3 from transakcje2

 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-ONE-PREPARE-SAMPLE-DATA----------------------------------------------
 ----------------------------------------------------------------------------------------------------------




 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-TWO-BUCKETS----------------------------------------------------------
 ----------------------------------------------------------------------------------------------------------



drop table if exists  #transaction_description


select 
bucket.case_id,
bucket.amount,
bucket.positive,
bucket.negative,
bucket.differance,
case 
	 when bucket.positive >0 and bucket.negative=0 then 'neutral'
	 when bucket.differance>0 then 'positive'
	 when bucket.differance<0 then 'negative'
	 when bucket.differance=0 then 'balancing' else null 
end [direction]
into #transaction_description

from 
	(select distinct case_id,
		   amount,
		   positive_m.cnt [positive],
		   negative_m.cnt [negative],
		   positive_m.cnt-negative_m.cnt [differance]
	  from transakcje3 t
	 outer apply (   select count(*) cnt
					   from transakcje3 tt
					  where t.case_id      = tt.case_id
						and abs(tt.amount) = t.amount
						and tt.amount      < 0) negative_m
	 outer apply (   select count(*) cnt
					   from transakcje3 tt
					  where t.case_id = tt.case_id
						and tt.amount = t.amount
						and tt.amount > 0) positive_m

	 where amount > 0 ) bucket

 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-TWO-BUCKETS----------------------------------------------------------
 ----------------------------------------------------------------------------------------------------------





 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-THREE-PREPARE-TABLE-WITHOUT-BALANCING-VALUES-------------------------
 ----------------------------------------------------------------------------------------------------------

 drop table if exists #table_without_balancing


 select sum(amount) [amount], count(*) [count] 
 into #table_without_balancing
 from (
 --neutral

 select t.case_id,t.amount,t.transaction_id from transakcje3 t
 where exists (
 select td.case_id,td.amount from #transaction_description td
 where direction='neutral' and t.case_id=td.case_id and t.amount=td.amount)

 --positive

  UNION ALL
 select b.case_id,b.amount,b.transaction_id from
 (select 
 t.case_id,
 t.amount,
 t.transaction_id,
 ROW_NUMBER() over (partition by t.case_id,t.amount order by  t.transaction_id) [row_numberr] 
 from transakcje3 t
 join (select t.case_id,t.amount from #transaction_description t  where direction='positive') td on t.case_id=td.case_id and t.amount=td.amount) b
 where exists (  select 1 from #transaction_description td where direction='positive' and b.case_id=td.case_id and b.amount=td.amount and b.row_numberr<=td.differance)



 --negative
 UNION ALL
  select b.case_id,b.amount,b.transaction_id from
 (select 
 t.case_id,
 t.amount,
 t.transaction_id,
 ROW_NUMBER() over (partition by t.case_id,t.amount order by  t.transaction_id) [row_numberr] 
 from transakcje3 t
 join (select t.case_id,t.amount from #transaction_description t  where direction='negative') td on t.case_id=td.case_id and t.amount=-td.amount) b
 where exists (  select 1 from #transaction_description td where direction='negative' and b.case_id=td.case_id and b.amount=-td.amount and b.row_numberr<=abs(td.differance))
 ) uninoned



 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-THREE-PREPARE-TABLE-WITHOUT-BALANCING-VALUES-------------------------
 ----------------------------------------------------------------------------------------------------------



 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-FOUR-PREPARE-COMPARE-RESULTS-----------------------------------------
 ----------------------------------------------------------------------------------------------------------



select sum(amount), count(*) from transakcje3

select amount,[count] from #table_without_balancing




 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-FOUR-PREPARE-COMPARE-RESULTS-----------------------------------------
 ----------------------------------------------------------------------------------------------------------



 ----------------------------------------------------------------------------------------------------------
 --------------------------------STEP-FIVE-DISTRIBUTION----------------------------------------------------
 ----------------------------------------------------------------------------------------------------------


select direction,count(*) from #transaction_description
group by direction



