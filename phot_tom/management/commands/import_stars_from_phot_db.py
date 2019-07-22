from django.core.management.base import BaseCommand
from tom_targets.models import Target, TargetExtra
from pyDANDIA import phot_db
from os import path
from astropy.coordinates import SkyCoord
from astropy import units as u

class Command(BaseCommand):
    
    help = 'Imports all stars from a pyDANDIA photometric database for a single field'
    
    def add_arguments(self, parser):
        parser.add_argument('--phot_db_path', help='Path to pyDANDIA photometry database')
        parser.add_argument('--field_name', help='Name of the field')
    
    def check_arguments(self, options):
        
        for key in ['phot_db_path', 'field_name']:
            
            if options[key] == None:
                raise ValueError('Missing argument '+key)
        
    def star_extra_params(self):
        
        star_keys = ['gaia_source_id', 'gaia_ra', 'gaia_ra_error', 'gaia_dec', 'gaia_dec_error', 
                 'gaia_phot_g_mean_flux', 'gaia_phot_g_mean_flux_error', 
                 'gaia_phot_bp_mean_flux', 'gaia_phot_bp_mean_flux_error', 
                 'gaia_phot_rp_mean_flux', 'gaia_phot_rp_mean_flux_error',
                 'vphas_source_id', 'vphas_ra', 'vphas_dec', 
                 'vphas_gmag', 'vphas_gmag_error', 
                 'vphas_rmag', 'vphas_rmag_error', 
                 'vphas_imag', 'vphas_imag_error', 'vphas_clean']
        
        return star_keys
    
    def check_star_in_tom(self,star_name):
        
        qs = Target.objects.filter(identifier=star_name)
        
        if len(qs) == 1:
            return qs[0]
        elif len(qs) > 1:
            raise IOError('Multiple database entries for star '+star_name)
        else:
            return None
    
    def get_target_extra_params(self,target):
        
        qs = TargetExtra.objects.filter(target=target)
        
        return qs
        
    def handle(self, *args, **options):
        
        self.check_arguments(options)
        
        errors = []
    
        if not path.isfile(options['phot_db_path']):
            raise IOError('Cannot find photometry database '+options['phot_db_path'])
        
        conn = phot_db.get_connection(dsn=options['phot_db_path'])
        
        pri_refimg = self.fetch_primary_reference_image_from_phot_db(conn)
        
        stars_table = self.fetch_starlist_from_phot_db(conn,pri_refimg)
        
        #pri_phot_table = self.fetch_primary_reference_photometry(conn,pri_refimg)
        
        for j,star in enumerate(stars_table):
            
            s = SkyCoord(star['ra'], star['dec'], 
                         frame='icrs', unit=(u.deg, u.deg))
            
            star_name = str(options['field_name'])+'-'+str(star['star_index'])
            
            base_params = {'identifier': star_name,
                            'name': star_name,
                            'ra': star['ra'],
                            'dec': star['dec'],
                            'galactic_lng': s.galactic.l.deg,
                            'galactic_lat': s.galactic.b.deg,
                            }
                        
            extra_params = { 'reference_image': pri_refimg['filename'][0] }
            for key in self.star_extra_params():
                extra_params[key] = star[key]
            
            
            #print(extra_params)
            #import pdb;pdb.set_trace()
            
            known_target = self.check_star_in_tom(star_name)
            
            if known_target == None:
                try:
                    target = Target.objects.create(**base_params)
                    print(' -> Created target for star '+star_name)
                    
                    for key, value in extra_params.items():
                        TargetExtra.objects.create(target=target, key=key, value=value)
                    print(' -> Ingested extra parameters')
                except OverflowError:
                    print(base_params,extra_params)
                    exit()
            else:
                
                try:
                    print(' -> '+star_name+' already in database')
                    
                    for key, value in base_params.items():
                        setattr(known_target,key,value)
                    known_target.save()
                    print(' -> Updated parameters for '+star_name)
                    
                    qs = self.get_target_extra_params(known_target)
                    
                    print(' -> Found '+str(len(qs))+' extra parameters for this target')
                    
                    for key, value in extra_params.items():
                        
                        for ts in qs:
                            if ts.key == key:
                                ts.value = value
                                ts.save()
                    print(' -> Updated extra parameters')
                except OverflowError:
                    print(base_params,extra_params)
                    exit()
                    
            #import pdb;pdb.set_trace()
            
            #except Exception as e:
            #    error = 'Error ingesting star '+str(j)+', '+str(options['field_name'])+'-'+str(star['star_index']),
            #    errors.append(error)
            
    def fetch_primary_reference_image_from_phot_db(self,conn):
        
        query = 'SELECT reference_image FROM stars'
        pri_refimg_id = phot_db.query_to_astropy_table(conn, query, args=())
        
        query = 'SELECT * FROM reference_images WHERE refimg_id="'+str(pri_refimg_id['reference_image'][0])+'"'
        pri_refimg = phot_db.query_to_astropy_table(conn, query, args=())
        
        return pri_refimg
    
    def fetch_starlist_from_phot_db(self,conn,pri_refimg):
        
        query = 'SELECT * FROM stars WHERE reference_image="'+str(pri_refimg['refimg_id'][0])+'"'
        stars_table = phot_db.query_to_astropy_table(conn, query, args=())

        print('Found '+str(len(stars_table))+' stars in the photometric database')
        
        return stars_table
    
    def fetch_primary_reference_photometry(self,conn,pri_refimg):
        
        query = 'SELECT * FROM phot WHERE reference_image="'+str(pri_refimg['refimg_id'][0])+'"'
        pri_phot_table = phot_db.query_to_astropy_table(conn, query, args=())
        
        print('Extracted '+str(len(pri_phot_table))+' photometric datapoints for the primary reference image')
        
        return pri_phot_table
        