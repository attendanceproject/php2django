
import php2django

from teams.models import Team

from localities.models import Locality

class team_locality_lookup:
    
    def do_lookup(self,index):
        return self.lookup_table[index]
    
    def __init__(self):
        self.lookup_table = {}
        lookup_table = {
            'Anaheim':[
                100, #Young People - Anaheim
                300, #Anaheim Children
                400, #Anaheim Community
                515, #Internet - Recovered to the Bible South America    I-RBSA
                       ],
            'Cerritos':[
                6, #Cerritos College
                502, #Young People - Cerritos
                        ],
            'Claremont':[
                8, #Claremont College
                         ],
            'Cypress':[
                9, #Cypress College
                       ],
            'Diamond Bar':[
                1, #Cal Poly Pomona
                12, #Mount San Antonio College
                104, #Young People - Diamond Bar
                514, #Diamond Bar - Community    DB-COM
                           ],
            'Fullerton':[
                2, #Cal State University Fullerton
                10, #Fullerton College
                105, #Young People - Fullerton
                511, #Internet - Defense and Confirmation Project    I-DCP
                516, #Fullerton - Community    FUL-COM
                         ],
            'Huntington Beach':[
                106, #Young People - Huntington Beach
                506, #Golden West College    GW
                                ],
            'Irvine':[
                16, #University of California Irvine
                107, #Young People - Irvine
                108, #Young People - Irvine / Junior High
                510, #Internet - Bibles for America    I-BFA
                519, #Irvine - Community    IRV-COM
                      ],
            'Long Beach':[
                503, #Long Beach
                505, #Young People - Long Beach    YP-LB
                          ],
            'Los Angeles':[
                4, #Cal State University Los Angeles
                17, #University of California, Los Angeles
                18, #University of Southern California
                           ],
            'Monterey Park':[
                501, #East LA College
                504, #Pasadena City College
                509, #California Institute of Technology    CIT
                       ],
            'Ontario':[
                109, #Young People - Ontario
                       ],
            'Orange':[
                7, #Chapman College
                13, #Orange Coast College
                517, #Santiago Canyon College    SCC
                      ],
            'Riverside':[
                518, #University of California Riverside    UCR
                         ],
            'San Juan Capistrano':[
                14, #Saddleback College
                512, #Young People - San Juan Capistrano    YP-SJC
                                   ], 
            'Santa Ana':[
                15, #Santa Ana College
                520, #Young People - Santa Ana    YP-SA
                         ],
            'Yorba Linda':[
                110, #Young People - Yorba Linda
                           ],
            'Walnut':[
                513, #Young People - Walnut    YP-WAL
                      ],
            # 500    Not Assigned (Part-termer)
            }
        for city_name, old_pks in lookup_table.iteritems():
            locality = Locality.objects.get(city__name=city_name)
            for old_pk in old_pks:
                self.lookup_table[old_pk] = locality

team_locality_lookup_instance = None

class ImportTeam(php2django.ImportTemplate):
    model=Team
    query='SELECT * FROM team'
    """
0    ID    int(11)
1    name    varchar(100)
2    code    varchar(15)
3    teamTypeID    int(11)
4    trainerUserName    varchar(45)
5    broMonitorTraineeID    int(11)
6    sisMonitorTraineeID    int(11)
    """
    # TODO write code to extract team monitor information from the team table
    # trainerUserName wasn't found to be used in any of the php code
    
    key=0
    
    def row_filter(self,row,importers):
        if row[0]==500: # remove "not assigned" team
            return False
        return True
    
    class mapping:
        # the full name of a team, e.g. Irvine Young People, or Anaheim Community
        name = 1 #models.CharField(max_length=50)
    
        # the abbreviation of the team, e.g. I-YP or ANA-COM
        code = 2 #models.CharField(max_length=10)
        """
        TEAM_TYPES = (
            ('CAMPUS', 'Campus'),
            ('CHILD', 'Children'),
            ('COM', 'Community'),
            ('YP', 'Young People'),
            ('I', 'Internet'),
        )
        teamTypeID
        1    Campus
        2    YP
        3    Children
        4    Community
        5    Part-termer
        6    Internet
        """
        #models.CharField(max_length=6, choices=TEAM_TYPES)
        def type(self,row,importers):
            return True
    
        # which locality this team is in
        def locality(self,row,importers): #models.ForeignKey(Locality)
            #TODO test this once the locality and city/address models are stable
            if not team_locality_lookup_instance:
                team_locality_lookup_instance = team_locality_lookup()
            team_locality_lookup_instance.do_lookup(row[0])
            return None
    
        # opposite of subteam... relates subteams to their superteam
        # There isn't a way to import this. It will have to be added after
        # superteam = models.ForeignKey('self', blank=True, null=True)
