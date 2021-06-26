select date_trunc('week',time) weekstamp, count(pull_requestid) 
from pr 
where event='opened' 
group by event,weekstamp 
order by weekstamp
