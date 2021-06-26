select a.name,b.monthnumber,b.countscore 
from(select hello.name, max(events) even, hello.monthnumber
from (select author as name,to_char(time, 'MM') monthnumber, count(event) events
from pr where event='discussed' 
group by to_char(time, 'MM'), author) as hello
group by hello.monthnumber,hello.name
order by monthnumber,even desc)as a, 
(select hello.monthnumber, max(events) countscore
from (select to_char(time, 'MM') monthnumber, count(event) events
from pr where event='discussed' 
group by to_char(time, 'MM'), author) as hello
group by hello.monthnumber)as b
where a.monthnumber=b.monthnumber and a.even=b.countscore









