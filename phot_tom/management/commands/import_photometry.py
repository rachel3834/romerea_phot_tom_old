# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 20:47:18 2019

@author: rstreet
"""

from django.core.management.base import BaseCommand
from tom_targets.models import Target, TargetExtra
from tom_dataproducts.models import ReducedDatum, DataProductGroup, DataProduct
from astropy.coordinates import SkyCoord
from astropy import units as u
from pyDANDIA import phot_db
from os import path
from datetime import datetime
import pytz
import json
from phot_tom.management.commands import import_utils

class Command(BaseCommand):
    
    help = 'Imports photometry data on stars from of the ROME/REA survey'

    def add_arguments(self, parser):
        parser.add_argument('--phot_db_path', help='Path to pyDANDIA photometry database')
        parser.add_argument('--field_name', help='Name of the field')
    
    def check_arguments(self, options):
        
        for key in ['phot_db_path', 'field_name']:
            
            if options[key] == None:
                raise ValueError('Missing argument '+key)
    
    def fetch_or_create_data_product_group(self):
        
        qs = DataProductGroup.objects.filter(name='romerea')
        
        if len(qs) == 0:
            group = DataProductGroup(**{'name': 'romerea'})
            group.save()
            
        else:
            group = qs[0]
        
        return group
    
    def fetch_star_from_tom(self,field_name,phot_db_id):
        
        star_name = str(field_name)+'-'+str(phot_db_id)
        
        qs = Target.objects.filter(identifier=star_name)
        
        if len(qs) == 1:
            return qs[0]
        elif len(qs) > 1:
            raise IOError('Multiple database entries for star '+star_name)
        else:
            return None
    
    def fetch_dataproduct(self,product_id):
        
        qs = DataProduct.objects.filter(product_id=product_id)
        
        if len(qs) > 0:
            return qs[0]
        else:
            return None
    
    def identify_datasets(self, conn, phot_table, star):
        
        lut = {}
        
        for entry in phot_table[0:10]:
                
            key = str(entry['facility'])+'_'+str(entry['filter'])
            
            if key in lut.keys():
                dataset = lut[key]
                
            else:
                dataset_code = import_utils.get_dataset_identifier(conn,entry)
                
                product_id = dataset_code+'_'+str(star['star_id'])
                product = self.fetch_dataproduct(product_id)
                
                dataset = {'product_id': product_id,
                           'product': product,
                           'dataset_code': dataset_code}
                          
                lut[key] = dataset
        
        return lut
        
    def clear_old_data(self, dataset_lut, target):
        
        for key, dataset in dataset_lut.items():
            
            qs = ReducedDatum.objects.filter(target=target, data_product=dataset['product'])
            print('Found '+str(len(qs))+' pre-existing Datums for '+str(target))
        
            for datum in qs:
                print('-> Removing '+str(datum))
                datum.delete()
        
    def handle(self, *args, **options):
            
        self.check_arguments(options)
        
        conn = phot_db.get_connection(dsn=options['phot_db_path'])
        
        pri_refimg = import_utils.fetch_primary_reference_image_from_phot_db(conn)
        
        stars_table = import_utils.fetch_starlist_from_phot_db(conn,pri_refimg)
        
        group = self.fetch_or_create_data_product_group()
        
        for star in stars_table[0:1]:
            
            phot_table = import_utils.fetch_photometry_for_star(conn,star['star_id'])
            
            target = self.fetch_star_from_tom(options['field_name'],star['star_id'])
            
            dataset_lut = self.identify_datasets(conn, phot_table, star)
            
            # Check for existing lightcurves for this star from the 
            # facilities listed in the phot_db.  If data are present, delete the 
            # associated Datums to clear the way for the new ingest
            self.clear_old_data(dataset_lut, target)
            
            for entry in phot_table[0:10]:
                
                key = str(entry['facility'])+'_'+str(entry['filter'])
                
                dataset = dataset_lut[key]

                if dataset['product'] == None:
                    data_file = path.basename(options['phot_db_path'])+'.'+dataset['product_id']
                
                    data_product_params = {"product_id": dataset['product_id'],
                                          "target": target,
                                          "observation_record": None,
                                          "data": data_file,  # This is used for uploaded file paths
                                          "extra_data": dataset['dataset_code'].split('_')[-1],
                                          "tag": "photometry",
                                          "featured": False,
                                        }
                
                    product = DataProduct.objects.create(**data_product_params)
                    product.group.add(group)
                
                    dataset[product] = product
                
                image = import_utils.get_image_entry(conn,entry['image'])
                date_obs = datetime.strptime(image['date_obs_utc'][0],"%Y-%m-%dT%H:%M:%S.%f")
                date_obs = date_obs.replace(tzinfo=pytz.UTC)
                
                value = {"magnitude": entry['calibrated_mag'],
                        "magnitude_error": entry['calibrated_mag_err'],
                        "hjd": entry['hjd'],
                        "filter": dataset['dataset_code'].split('_')[-1]}
                                    
                datum_params = {"target": target,
                              "data_product": dataset['product'],
                              "data_type": "photometry",
                              "source_name": dataset['product_id'],
                              "source_location": key,
                              "timestamp": date_obs,
                              "value": json.dumps(value),
                              }
                            
                datum = ReducedDatum.objects.create(**datum_params)
                