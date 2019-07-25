# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 20:32:32 2019

@author: rstreet
"""
import django_filters
from django.db.models import Q
from django.conf import settings

from tom_targets.models import Target

class StarTargetFilter(django_filters.FilterSet):
    
    key = django_filters.CharFilter(field_name='targetextra__key', label='Key')
    value = django_filters.CharFilter(field_name='targetextra__value', label='Value')

    def __init__(self, *args, **kwargs):
        
        super().__init__(*args, **kwargs)

        extra_fields = [ 'rome_field', 'reference_image', 'target_type',
                        'gaia_source_id', 'vphas_source_id' ]
        for field in settings.EXTRA_FIELDS:
            new_filter = filter_for_field(field)
            new_filter.parent = self
            self.filters[field['name']] = new_filter

    identifier = django_filters.CharFilter(field_name='identifier', lookup_expr='icontains')

    class Meta:
        model = Target
        fields = ['type', 'ra', 'dec', 'identifier', 'name']
