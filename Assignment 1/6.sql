select date(time), count(pull_requestId) 
from pr 
group by date(time) 
order by date(time)