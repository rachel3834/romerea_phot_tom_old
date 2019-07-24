from django import template
from astropy.coordinates import SkyCoord
from astropy import units as u

from tom_targets.models import Target, TargetExtra

register = template.Library()


@register.inclusion_tag('custom_views/fields_table.html')
def fields_table():

    table_columns = [ 'Field', 'RA Centre', 'Dec Centre', 
                      'N stars' ]
    
    table_rows = []

    targetextras=TargetExtra.objects.filter(key='target_type', value='"field"')
    
    for e in targetextras:
        
        f = SkyCoord(e.target.ra, e.target.dec, 
                     frame='icrs', unit=(u.deg, u.deg))
       
        stars = TargetExtra.objects.filter(key='rome_field', value=e.target.name)
        
        table_rows.append( [e.target.id, e.target.name, f.ra.to_string(unit=u.hourangle, sep=':'), 
                            f.dec.to_string(unit=u.degree, sep=':'), 
                            len(stars)] )
    
    return {'table_columns': table_columns, 'table_rows': table_rows}

@register.inclusion_tag('custom_views/display_field_image.html')
def field_image(target):
    
    image_file = 'img/'+str(target.name)+'_colour.png'
    
    return {'target_image': image_file}