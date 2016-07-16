import os
import re
import sys

import php2django

from aputils.models import City, Address, HomeAddress, Vehicle, EmergencyInfo

# TODO
class ImportCity(php2django.ImportTemplate):
    model=City
    #TODO fix/write this query
    query='SELECT city, state as region, null as country FROM residence'
    #key=0
    
    class mapping:
        name = -1 
        

class ImportAddress(php2django.ImportTemplate):
    model=Address
    query='SELECT * FROM residence'
    # query='SELECT streetAddress as address1, city, state, zipCode as zip_code, null as details FROM residence'
    """
0    ID    int(11)
1    name    varchar(45)
2    residenceTypeID    
3    capacity    int(11)
4    streetAddress    varchar(45)
5    city    varchar(45)
6    state
7    zipCode
8    phone
9
10
11   fttaUse    
    """
    key=0
    
    def row_filter(self,row,importers):
        # remove houses not for brothers/sisters/couples
        if not row[4]:
            return False
        return True

    class mapping:

        address1=4
        def city(self,row,importers):
            city = City.objects.filter(name=row[5]).first()
            if city:
                return city
            else:
                return City.objects.filter(name='Anaheim').first()
        zip_code=7

vhcl_make = re.compile('|'.join((
    'Acura','Accra',
    'Audi',
    'Bmw',
    'Buick','Buik',
    'Cadillac',
    'Chevrolet','Chevorlet','Chevy',
    'Chrysler',
    'Dodge',
    'Ford',
    'Geo',
    'Gmc',
    'Honda',
    'Hyundai','Hyndai',
    'Infiniti','Infinity',
    'Isuzu',
    'Jeep',
    'Kia',
    'Lexus',
    'Lincoln',
    'Mazda',
    'Mercedes','Mb2',
    'Mercury',
    'Mini',
    'Mitsubishi','Misibishi',
    'Nissan',
    'Oldsmobile',
    'Plymouth',
    'Pontiac',
    'Saab',
    'Saturn',
    'Scion','Zion',
    'Subaru',
    'Suzuki',
    'Toyota','Toyata',
    'Volare',
    'Volvo',
    'Volkswagen','Vw','Volkswagon',
    )))
vhcl_make_replace = {
    'Accra':'Acura',
    'Bmw':'BMW',
    'Buik':'Buick',
    'Chevy':'Chevrolet',
    'Chevorlet':'Chevrolet',
    'Gmc':'GMC',
    'Hyndai':'Hyundai',
    'Infinity':'Infiniti',
    'Mb2':'Mercedes',
    'Misibishi':'Mitsubishi',
    'Zion':'Scion',
    'Toyata':'Toyota',
    'Volkswagon':'Volkswagen',
    'Vw':'Volkswagen',
    }
vhcl_model_to_make = (
    ('Accord','Honda'),
    ('Camry','Toyota'),
    ('Corolla','Toyota'),
    ('Yukon','GMC'),
    ('Cobolt','Chevrolet'),
    )
vhcl_model_replace = (
    (re.compile('^Cobolt'),'Cobalt'),
    (re.compile('^Lesabre'),'LeSabre'),
    (re.compile('^Rav'),'RAV'),
    (re.compile('^Cr[-]?[vV]'),'CR-V'),
    (re.compile('^Xl7'),'XL7'),
    (re.compile('^Xd'),'xD'),
    (re.compile('([0-9])I$'),r"\1i"),
    (re.compile('[Ee]s(\\s|[0-9]|$)'),r"ES\1"),
    (re.compile('Dh$'),'DH'),
    (re.compile('Dx$'),'DX'),
    (re.compile('Gt$'),'GT'),
    (re.compile('Gti$'),'GTI'),
    (re.compile('Le$'),'LE'),
    (re.compile('Lx$'),'LX'),
    (re.compile('Is([0-9]|$)'),r"IS\1"),
    (re.compile('Rsx$'),'RSX'),
    (re.compile('Rx300$'),'RX300'),
    (re.compile('Se$'),'SE'),
    (re.compile('Sts$'),'STS'),
    (re.compile('Xc90$'),'XC90'),
    
    )

separators = re.compile('(?:[/,]\s*)')

vhcl_year = re.compile('(?:[0-9]{2}|\'|^)[0-9]{2}')

plate_state = re.compile('\\(([A-Za-z]*)\\)')

garbage = re.compile('^-')

def sanitize(string):
    string = re.sub(separators,' ',str(string)).replace('\xe9','e').replace('\xa8\xa6', 'e').replace('/', ' ').title()
    return string

class ImportVehicle(php2django.ImportTemplate):
    model=Vehicle
    query='SELECT t.ID, t.vehicleInfoOld, t.vehicleMakeOld, t.vehicleModelOld, t.vehicleYearOld, t.vehicleYesNo, t.vehicleModel, t.vehicleLicense, t.vehicleColor, t.vehicleCapacity FROM trainee t JOIN user u ON t.userID=u.ID AND u.country<>"New Jerusalem" WHERE t.vehicleCapacity>0'
    """
0  traineeID    int(10)
1  vehicleInfoOld    varchar(50)
2  vehicleMakeOld    varchar(255)
3  vehicleModelOld    varchar(255)
4  vehicleYearOld    int(11)
5  vehicleYesNo    tinyint(1)
6  vehicleModel    varchar(50)
7  vehicleLicense    varchar(50)
8  vehicleColor    varchar(50)
9  vehicleCapacity    double(15,5) 
    """
    
    key=0
    
    class mapping:
        #models.CharField(max_length=10)
        def color(self,row,importers):
            color = sanitize(row[8])
            color = re.sub(vhcl_year, '', color).strip()
            return php2django.truncate_str(color,10) if row[8] else ''

        # e.g. "Honda", "Toyota"
        #models.CharField(max_length=30)
        def make(self,row,importers):
            if row[6]:
                info = sanitize(row[6])
            elif row[2]:
                info = sanitize(row[2])
            elif row[1]:
                info = sanitize(row[1])
            else:
                return ''
            match = re.search(vhcl_make, info)
            if match is None:
                for model, make in vhcl_model_to_make:
                    if info.find(model)!=-1 or sanitize(row[6]).find(model)!=-1:
                        return make
            if match:
                make = match.group(0)
                if make in vhcl_make_replace:
                    return vhcl_make_replace[make]
                return make
            sys.stderr.write('WARNING: Could not identify model for %s\n' % (str(row)))
            return '' 
    
        # e.g. "Accord", "Camry"
        #models.CharField(max_length=30)
        def model(self,row,importers):
            info = None
            for index in [6,3,1]:
                if row[index] is None: continue
                info = sanitize(row[index])
                info = re.sub(vhcl_make, '', info)
                info = re.sub(vhcl_year, '', info)
                info = re.sub(',', ' ', info)
                info = re.sub('  ', ' ', info)
                info = re.sub(garbage, '', info)
                info = info.strip()
                if info != '': break
            if info and info=='':
                raise Exception('No model: %s' % (info))
            if info is None or info in ['Yes','English']:
                return ''
            for reg, replacement in vhcl_model_replace:
                info = re.sub(reg, replacement, info)
            return '' if info is None else info
    
        #year = models.PositiveSmallIntegerField()
        def year(self,row,importers):
            info = None
            for index in [4,6,3,1,8]:
                if row[index] is None: continue
                info = sanitize(row[index])
                match = re.search(vhcl_year, info)
                if match is None:
                    info=None
                    continue
                info = match.group(0).replace('\'','')
                if info != '': break
            if info and info=='':
                raise Exception('No year: %s' % (info))
            if info is None:
                return '0'
            return info
    
        #models.CharField(max_length=10)
        def license_plate(self,row,importers):
            plate_no = str(row[7])
            plate_no = re.sub(plate_state,'',plate_no).strip()
            return php2django.truncate_str(plate_no,10) if row[7] else ''
    
        def state(self,row,importers):
            match = re.search(plate_state, str(row[7]))
            if match is None:
                return ''
            return match.group(1)
            
        #state = models.CharField(max_length=20)
        
        capacity=9 
    
        #models.OneToOneField('accounts.Trainee', blank=True, null=True)
        trainee=0