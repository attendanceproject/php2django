import datetime

from php2django import php2django

from terms.models import Term

def parse_date(date): # parses date of the format %Y-%m-%d
    return datetime.date(*[int(token) for token in (date.split('-'))])

class ImportTerm(php2django.ImportTemplate):
    model=Term
    query='SELECT * FROM term'
    """
0    ID    int(11)
1    name    varchar(45)
2    startDate    char(10)
3    endDate    char(10)
4    taskStage    int(11)
5    tasksCompleted    int(11)
    """
    
    key=0
    
    class mapping:
        def season(self,row,**kargs):
            if row[1].startswith('Spring'):
                return 'Spring'
            if row[1].startswith('Fall'):
                return 'Fall'
        def year(self,row,**kargs):
            return parse_date(row[2]).year
        def start(self,row,**kargs):
            return parse_date(row[2])
        def end(self,row,**kargs):
            return parse_date(row[3])
