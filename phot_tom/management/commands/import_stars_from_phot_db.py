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
        
    def handle(self, *args, **options):
        
        self.check_arguments(options)
        
        errors = []
    
        if not path.isfile(options['phot_db_path']):
            raise IOError('Cannot find photometry database '+options['phot_db_path'])
        
        conn = phot_db.get_connection(dsn=options['phot_db_path'])
        
        pri_refimg = self.fetch_primary_reference_image_from_phot_db(conn)
        
        stars_table = self.fetch_starlist_from_phot_db(conn,pri_refimg)
        
        #pri_phot_table = self.fetch_primary_reference_photometry(conn,pri_refimg)
        
        for j,star in enumerate(stars_table[:10]):
            
            s = SkyCoord(star['ra'], star['dec'], 
                         frame='icrs', unit=(u.deg, u.deg))
            
            base_params = {'identifier': str(options['field_name'])+'-'+str(star['star_index']),
                            'name': str(options['field_name'])+'-'+str(star['star_index']),
                            'ra': star['ra'],
                            'dec': star['dec'],
                            'galactic_lng': s.galactic.l.deg,
                            'galactic_lat': s.galactic.b.deg,
                            }
                        
            extra_params = [ ('reference_image',pri_refimg['filename'][0]) ]
            for key in self.star_extra_params():
                extra_params.append( ( key, star[key] ) )
            
            print(extra_params)
            
            #print(extra_params)
            #import pdb;pdb.set_trace()
            
            #try:
            target = Target.objects.create(**base_params)
            print(target)
            print(' -> Created target for star '+str(options['field_name'])+'-'+str(star['star_index']))
            
            for extra in extra_params:
                print(target,extra[0])
                TargetExtra.objects.create(target=target, key=extra[0], value=extra[1])
            
            print(' -> Ingested extra parameters')
            
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
        