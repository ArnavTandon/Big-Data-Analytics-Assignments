select to_char(time, 'MM-YYYY') mon, count(event) 
from pr 
where event='merged' and extract(year from time)=2010 
group by mon