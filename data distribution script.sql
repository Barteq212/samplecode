

drop table if exists #data_table

select  
top 10000
-ABS(checksum(newid()))%360001.12  [column_data]
into #data_table
from sys.all_columns,sys.all_columns c


insert into #data_table
select  
top 10000
-ABS(checksum(newid()))%3601.12  [column_data]
from sys.all_columns,sys.all_columns c


insert into #data_table
select  
top 10000
ABS(checksum(newid()))%3601.12  [column_data]
from sys.all_columns,sys.all_columns c


insert into #data_table
select  
top 10000
ABS(checksum(newid()))%36001.12  [column_data]
from sys.all_columns,sys.all_columns c




declare @step int = 264
declare @step_len int = 500
declare @column_name nvarchar(100) = 'column_data'
declare @table_name nvarchar(1000) = '#data_table'
declare @negative nvarchar(max) = ''
declare @positive nvarchar(max) = ''
declare @full_str nvarchar(max) = ''



select @positive
    = CONCAT_WS(
                   ' ',
                   'select distribution, count (*) [how_many], ''positive'' [direction] from (',
                   e.before_count,
                   ' )f where distribution is not null  group by [distribution]'
               )
from
(
    select CONCAT_WS(' ', 'select', @column_name, ',case ') + STRING_AGG(cast(d.case_when as nvarchar(max)), ' ')
           +
           (
               select concat(
                                ' when ',
                                @column_name,
                                '>',
                                (@step_len + 1) * @step,
                                'then ',
                                '''',
                                '>',
                                (@step_len + 1) * @step,
                                '''',
                                ' end ',
                                '[distribution]'
                            )
           ) + CONCAT_WS(' ', 'from', @table_name) [before_count]
    from
    (
        select concat(
                         'when ',
                         @column_name,
                         c.sign_start,
                         range_start,
                         ' and ',
                         @column_name,
                         c.sign_stop,
                         c.range_stop,
                         ' then ',
                         '''',
                         range_start,
                         '-',
                         range_stop,
                         ''''
                     ) [case_when]
        from
        (
            select 0 [range_start],
                   @step [range_stop],
                   '>=' [sign_start],
                   '<=' [sign_stop]
            union all
            select b.steps_number * @step [range_start],
                   (b.steps_number + 1) * @step [range_stop],
                   '>' [sign_start],
                   '<=' [sign_stop]
            from
            (
                select top (@step_len)
                    ROW_NUMBER() over (order by (select null)) [steps_number]
                from sys.all_columns,
                     sys.all_columns a
            ) b
        ) c
    ) d
) e



select @negative
    = CONCAT_WS(
                   ' ',
                   'select distribution, count (*) [how_many], ''negative'' [direction] from (',
                   e.before_count,
                   ' )f where distribution is not null group by [distribution]'
               )
from
(
    select CONCAT_WS(' ', 'select', @column_name, ',case ') + STRING_AGG(cast(d.case_when as nvarchar(max)), ' ')
           +
           (
               select concat(
                                ' when ',
                                @column_name,
                                '<-',
                                (@step_len + 1) * @step,
                                'then ',
                                '''',
                                '<',
                                (@step_len + 1) * @step,
                                '''',
                                ' end ',
                                '[distribution]'
                            )
           ) + CONCAT_WS(' ', 'from', @table_name) [before_count]
    from
    (
        select concat(
                         'when ',
                         @column_name,
                         c.sign_start,
                         range_start,
                         ' and ',
                         @column_name,
                         c.sign_stop,
                         c.range_stop,
                         ' then ',
                         '''',
                         range_start,
                         '-',
                         range_stop,
                         ''''
                     ) [case_when]
        from
        (
            select 0 [range_start],
                   @step [range_stop],
                   '<' [sign_start],
                   '>=-' [sign_stop]
            union all
            select b.steps_number * @step [range_start],
                   (b.steps_number + 1) * @step [range_stop],
                   '<-' [sign_start],
                   '>=-' [sign_stop]
            from
            (
                select top (@step_len)
                    ROW_NUMBER() over (order by (select null)) [steps_number]
                from sys.all_columns,
                     sys.all_columns a
            ) b
        ) c
    ) d
) e


set @full_str
    = 'select * from ( ' + @positive + ' union all ' + @negative
      + ' )b
order by direction desc, case 
when CHARINDEX(''-'',distribution,1) >0 then left(distribution,charindex(''-'',distribution,1)-1)
when CHARINDEX(''-'',distribution,1) = 0 then right(distribution,len(distribution)-1)
else 0 end asc'



exec (@full_str)




