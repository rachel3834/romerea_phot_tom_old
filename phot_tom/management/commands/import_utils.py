# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 20:51:42 2019

@author: rstreet
"""
from pyDANDIA import phot_db

def fetch_primary_reference_image_from_phot_db(conn):
    
    query = 'SELECT reference_image FROM stars'
    pri_refimg_id = phot_db.query_to_astropy_table(conn, query, args=())
    
    query = 'SELECT * FROM reference_images WHERE refimg_id="'+str(pri_refimg_id['reference_image'][0])+'"'
    pri_refimg = phot_db.query_to_astropy_table(conn, query, args=())
    
    return pri_refimg

def fetch_starlist_from_phot_db(conn,pri_refimg):
    
    query = 'SELECT * FROM stars WHERE reference_image="'+str(pri_refimg['refimg_id'][0])+'"'
    stars_table = phot_db.query_to_astropy_table(conn, query, args=())

    print('Found '+str(len(stars_table))+' stars in the photometric database')
    
    return stars_table

def fetch_primary_reference_photometry(conn,pri_refimg):
    
    query = 'SELECT * FROM phot WHERE reference_image="'+str(pri_refimg['refimg_id'][0])+'"'
    pri_phot_table = phot_db.query_to_astropy_table(conn, query, args=())
    
    print('Extracted '+str(len(pri_phot_table))+' photometric datapoints for the primary reference image')
    
    return pri_phot_table

def fetch_photometry_for_dataset(conn,ref_image_file):
    
    query = 'SELECT * FROM reference_images WHERE filename="'+str(ref_image_file)+'"'
    refimg = phot_db.query_to_astropy_table(conn, query, args=())
    
    query = 'SELECT * FROM phot WHERE reference_image="'+str(refimg)+'"'
    phot_table = phot_db.query_to_astropy_table(conn, query, args=())
    
    return phot_table