from django import template
from astropy.coordinates import SkyCoord
from astropy import units as u
from plotly import offline
import plotly.graph_objs as go
import numpy as np

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

@register.inclusion_tag('custom_views/partials/field_distribution.html')
def field_distribution():

    targetextras=TargetExtra.objects.filter(key='target_type', value='"field"')

    field_width = 26.0/60.0

    locations = []
    min_ra = 1e4
    max_ra = -1e4
    min_dec = 1e4
    max_dec = -1e4
    for e in targetextras:
        locations.append( (e.target.ra, e.target.dec, e.target.name) )
        min_ra = min(min_ra, (e.target.ra-field_width))
        max_ra = max(max_ra, (e.target.ra+field_width))
        min_dec = min(min_dec, (e.target.dec-field_width))
        max_dec = max(max_dec, (e.target.dec+field_width))

    if max_ra > min_ra:
        dra = max_ra - min_ra
    else:
        dra = 0.0
        min_ra = 0.0
        max_ra = 0.0
    if max_dec > min_dec:
        ddec = max_dec - min_dec
    else:
        ddec = 0.0
        min_dec = 0.0
        max_dec = 0.0
    plot_width = max(dra, ddec)

    if plot_width != 0.0:
        delta_ra = (max_ra - min_ra)/5.0
        delta_dec = (max_dec - min_dec)/5.0
    else:
        plot_width = 10.0
        delta_ra = 1.0
        delta_dec = 1.0
    pixscale = plot_width / 250

    data = [
        dict(
            lon=[l[0] for l in locations],
            lat=[l[1] for l in locations],
            text=[l[2] for l in locations],
            hoverinfo='lon+lat+text',
            mode='markers',
            type='scattergeo',
            marker={'symbol': 'square-open', 'size': field_width/pixscale}
            #, 'size': field_width, 'opacity': 1,
            #        'color': 'blue'}
        )
    ]
    layout = {
        'title': 'Distribution of Survey Fields',
        'hovermode': 'closest',
        'showlegend': False,
        'geo': {
            'projection': {
                'type': 'mollweide',
            },
            'showcoastlines': False,
            'showland': False,
            'lonaxis': {
                'showgrid': True,
                'range': [min_ra, max_ra],
                'dtick': delta_ra,
            },
            'lataxis': {
                'showgrid': True,
                'range': [min_dec, max_dec],
                'dtick': delta_dec,
            },
        }
    }
    figure = offline.plot(go.Figure(data=data, layout=layout), output_type='div', show_link=False)
    return {'figure': figure}
