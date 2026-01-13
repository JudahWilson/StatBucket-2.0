from django.contrib import admin
from django.apps import apps

################################################################################
# Dynamic model registration shown in singular form
################################################################################
app_models = apps.get_app_config('app').get_models()
for model in app_models:
    try:
        # Create admin class that overrides the plural name display
        class DynamicModelAdmin(admin.ModelAdmin):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                # Override the model's verbose_name_plural
                self.model._meta.verbose_name_plural = model.__name__
        
        admin.site.register(model, DynamicModelAdmin)
    except admin.sites.AlreadyRegistered:
        pass  # Skip if already registered