# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 20:51:42 2019

@author: rstreet
"""
from pyDANDIA import phot_db
from tom_targets.models import Target

def fetch_primary_reference_image_from_phot_db(conn):

    query = 'SELECT reference_image FROM stars'
    pri_refimg_id = phot_db.query_to_astropy_table(conn, query, args=())

    query = 'SELECT * FROM reference_images WHERE refimg_id="'+str(pri_refimg_id['reference_image'][0])+'"'
    pri_refimg = phot_db.query_to_astropy_table(conn, query, args=())

    return pri_refimg

def fetch_starlist_from_phot_db(conn,pri_refimg,log=None):

    query = 'SELECT * FROM stars WHERE reference_image="'+str(pri_refimg['refimg_id'][0])+'"'
    stars_table = phot_db.query_to_astropy_table(conn, query, args=())

    if log != None:
        log.info('Found '+str(len(stars_table))+' stars in the photometric database')

    return stars_table

def fetch_primary_reference_photometry(conn,pri_refimg):

    query = 'SELECT * FROM phot WHERE reference_image="'+str(pri_refimg['refimg_id'][0])+'"'
    pri_phot_table = phot_db.query_to_astropy_table(conn, query, args=())

    print('Extracted '+str(len(pri_phot_table))+' photometric datapoints for the primary reference image')

    return pri_phot_table

def fetch_dataset_list(conn):

    query = 'SELECT * FROM reference_images'
    datasets = phot_db.query_to_astropy_table(conn, query, args=())

    return datasets

def fetch_photometry_for_dataset(conn,ref_image_file):

    query = 'SELECT * FROM reference_images WHERE filename="'+str(ref_image_file)+'"'
    refimg = phot_db.query_to_astropy_table(conn, query, args=())

    query = 'SELECT * FROM phot WHERE reference_image="'+str(refimg['refimg_id'][0])+'"'
    phot_table = phot_db.query_to_astropy_table(conn, query, args=())

    return phot_table

def fetch_photometry_for_star(conn, star_id, log=None):

    query = 'SELECT * FROM phot WHERE star_id="'+str(star_id)+'"'
    phot_table = phot_db.query_to_astropy_table(conn, query, args=())

    if log != None:
        log.info('Extracted '+str(len(phot_table))+' datapoints for star '+str(star_id))

    return phot_table

def get_dataset_identifier(conn,entry):

    query = 'SELECT * FROM facilities WHERE facility_id="'+str(entry['facility'])+'"'
    facility = phot_db.query_to_astropy_table(conn, query, args=())

    query = 'SELECT * FROM filters WHERE filter_id="'+str(entry['filter'])+'"'
    f = phot_db.query_to_astropy_table(conn, query, args=())

    dataset_code = facility['facility_code'][0]+'_'+f['filter_name'][0]

    return dataset_code

def get_image_entry(conn,image_id):

    query = 'SELECT * FROM images WHERE img_id="'+str(image_id)+'"'
    image = phot_db.query_to_astropy_table(conn, query, args=())

    return image
