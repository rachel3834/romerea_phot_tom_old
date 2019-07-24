# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 11:01:54 2019

@author: rstreet
"""

from django.core.management.base import BaseCommand
from tom_targets.models import Target, TargetExtra
from astropy.coordinates import SkyCoord
from astropy import units as u

class Command(BaseCommand):
    
    help = 'Imports data on the target fields of the ROME/REA survey'
    
    def get_field_params(self):
        """Definition of the ROME survey target fields, taken from robonet_site"""
        
        fields={'ROME-FIELD-01':[ 267.835895375 , -30.0608178195 , '17:51:20.6149','-30:03:38.9442' ],
            'ROME-FIELD-02':[ 269.636745458 , -27.9782661111 , '17:58:32.8189','-27:58:41.758' ],
            'ROME-FIELD-03':[ 268.000049542 , -28.8195573333 , '17:52:00.0119','-28:49:10.4064' ],
            'ROME-FIELD-04':[ 268.180171708 , -29.27851275 , '17:52:43.2412','-29:16:42.6459' ],
            'ROME-FIELD-05':[ 268.35435 , -30.2578356389 , '17:53:25.044','-30:15:28.2083' ],
            'ROME-FIELD-06':[ 268.356124833 , -29.7729819283 , '17:53:25.47','-29:46:22.7349' ],
            'ROME-FIELD-07':[ 268.529571333 , -28.6937071111 , '17:54:07.0971','-28:41:37.3456' ],
            'ROME-FIELD-08':[ 268.709737083 , -29.1867251944 , '17:54:50.3369','-29:11:12.2107' ],
            'ROME-FIELD-09':[ 268.881108542 , -29.7704673333 , '17:55:31.4661','-29:46:13.6824' ],
            'ROME-FIELD-10':[ 269.048498333 , -28.6440675 , '17:56:11.6396','-28:38:38.643' ],
            'ROME-FIELD-11':[ 269.23883225 , -29.2716684211 , '17:56:57.3197','-29:16:18.0063' ],
            'ROME-FIELD-12':[ 269.39478875 , -30.0992361667 , '17:57:34.7493','-30:05:57.2502' ],
            'ROME-FIELD-13':[ 269.563719375 , -28.4422328996 , '17:58:15.2927','-28:26:32.0384' ],
            'ROME-FIELD-14':[ 269.758843 , -29.1796030365 , '17:59:02.1223','-29:10:46.5709' ],
            'ROME-FIELD-15':[ 269.78359875 , -29.63940425 , '17:59:08.0637','-29:38:21.8553' ],
            'ROME-FIELD-16':[ 270.074981708 , -28.5375585833 , '18:00:17.9956','-28:32:15.2109' ],
            'ROME-FIELD-17':[ 270.81 , -28.0978333333 , '18:03:14.4','-28:05:52.2' ],
            'ROME-FIELD-18':[ 270.290886667 , -27.9986032778 , '18:01:09.8128','-27:59:54.9718' ],
            'ROME-FIELD-19':[ 270.312763708 , -29.0084241944 , '18:01:15.0633','-29:00:30.3271' ],
            'ROME-FIELD-20':[ 270.83674125 , -28.8431573889 , '18:03:20.8179','-28:50:35.3666' ]}

        return fields
    
    def check_field_in_tom(self,field_id):
        
        qs = Target.objects.filter(identifier=field_id)
        
        if len(qs) == 1:
            return qs[0]
        elif len(qs) > 1:
            raise IOError('Multiple database entries for star '+field_id)
        else:
            return None
            
    def handle(self, *args, **options):
        
        fields = self.get_field_params()
        
        for field_id,field_pars in fields.items():
            
            field = self.check_field_in_tom(field_id)
            
            if field == None:
                
                f = SkyCoord(field_pars[0], field_pars[1], 
                             frame='icrs', unit=(u.deg, u.deg))
                             
                base_params = {'identifier': field_id,
                                'name': field_id,
                                'ra': field_pars[0],
                                'dec': field_pars[1],
                                'galactic_lng': f.galactic.l.deg,
                                'galactic_lat': f.galactic.b.deg,
                                'type': Target.SIDEREAL,
                                }
                
                target = Target.objects.create(**base_params)
                
                TargetExtra.objects.create(target=target, key='target_type', value='"field"')
                
                print(' -> Ingested information for field '+field_id)
                