# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 20:47:18 2019

@author: rstreet
"""

from django.core.management.base import BaseCommand
from tom_targets.models import Target, TargetExtra
from tom_dataproducts import ReducedDatum, DataProductGroup
from astropy.coordinates import SkyCoord
from astropy import units as u
from pyDANDIA import phot_db
from os import path
import import_utils

class Command(BaseCommand):
    
    help = 'Imports photometry data on stars from of the ROME/REA survey'

    def add_arguments(self, parser):
        parser.add_argument('--phot_db_path', help='Path to pyDANDIA photometry database')
        parser.add_argument('--field_name', help='Name of the field')
        parser.add_argument('--ref_image_file', help='Filename of the reference image for a specific dataset')
    
    def check_arguments(self, options):
        
        for key in ['phot_db_path', 'field_name']:
            
            if options[key] == None:
                raise ValueError('Missing argument '+key)
    
    def fetch_or_create_data_product_group(self):
        
        qs = DataProductsGroup.objects.filter(name='romerea')
        
        if len(qs) == 0:
            group = DataProductGroup('name': 'romerea')
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
        
        if len(qs) == 0:
            return qs[0]
        else:
            return None
            
    def handle(self, *args, **options):
            
        self.check_arguments(options)
        
        conn = phot_db.get_connection(dsn=options['phot_db_path'])
        
        pri_refimg = import_utils.fetch_primary_reference_image_from_phot_db(conn)
                
        phot_table = import_utils.fetch_photometry_for_dataset(conn,options['ref_image_file'])
        
        group = self.fetch_or_create_data_product_group()
        
        stars_table = {}
        products_table = {}
        
        for entry in phot_table:
            
            if entry.star_id not in stars_table.keys():
                star = self.fetch_star_from_tom(options['field_name'],entry.star_id)
                stars_table[entry.star_id] = star
            else:
                star = stars_table[entry.star_id]
            
            product_id = entry.facility+'_'+entry.filter.filter_name

            if star != None:
                
                product = self.fetch_dataproduct(product_id)
                
                if product == None:
                    data_product_params = {'product_id': product_id,
                                          'target': star,
                                          'observation_record': None,
                                          'data': None,  # This is used for uploaded file paths
                                          'extra_data': entry.filter.filter_name,
                                          'group': group,
                                          'tag': 'PHOTOMETRY',
                                          'featured': False,
                                        }
                    product = DataProduct.objects.create(**data_product_params)
                
                datum_params = {'target': star,
                          'dataproduct': product,
                          'producttype': 'PHOTOMETRY',
                          'source_name': 
                          'source_location':
                          'timestamp':
                          'value': 
                        }
                datum = ReducedDatum.objects.create(**datum_params)
            
            else:
                print('Skipping ingest for unknown star '+str(entry.star_id))
                