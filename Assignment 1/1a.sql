select event,date(time),count(*) 
from pr 
group by event,date(time)
having event='opened'
order by date(time)