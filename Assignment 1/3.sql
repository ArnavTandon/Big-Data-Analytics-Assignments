select a.name,b.week,b.countscore 
from(select hello.name, max(events) even, hello.week
from (select author as name,date_trunc('week',time) week, count(event) events
from pr where event='discussed' 
group by date_trunc('week',time), author) as hello
group by hello.week,hello.name
order by week,even desc)as a, 
(select hello.week, max(events) countscore
from (select date_trunc('week',time) week, count(event) events
from pr where event='discussed' 
group by date_trunc('week',time), author) as hello
group by hello.week)as b
where a.week=b.week and a.even=b.countscore