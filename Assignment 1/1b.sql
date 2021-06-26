select event,date(time),count(*) 
from pr 
group by event,date(time) 
having event='discussed'
order by date(time)