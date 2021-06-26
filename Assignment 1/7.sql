select author, count(event) events 
from pr 
where event='opened' and extract(year from time)=2011 
group by author 
order by events desc 
limit 1
