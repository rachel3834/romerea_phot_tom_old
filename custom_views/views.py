from django.shortcuts import render
from tom_targets.views import TargetDetailView

# Create your views here.
class FieldDetailView(TargetDetailView):
    permission_required = 'tom_targets.view_target'
    model = Target
    template_name = 'custom_views/field_summary.html'
    