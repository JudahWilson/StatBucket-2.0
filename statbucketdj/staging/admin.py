from django.contrib import admin
from django.apps import apps

################################################################################
# Dynamic Model Registration for Staging
################################################################################
staging_models = apps.get_app_config('staging').get_models()
for model in staging_models:
    try:
        # Create admin class that uses singular name for both singular and plural
        class DynamicModelAdmin(admin.ModelAdmin):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                # Override the model's verbose_name_plural
                self.model._meta.verbose_name_plural = model.__name__
        
        admin.site.register(model, DynamicModelAdmin)
    except admin.sites.AlreadyRegistered:
        pass  # Skip if already registered
