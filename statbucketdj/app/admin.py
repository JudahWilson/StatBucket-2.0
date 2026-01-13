from django.contrib import admin
from django.apps import apps

################################################################################
# Database-aware admin with visual database indicators
################################################################################
class MultiDatabaseModelAdmin(admin.ModelAdmin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Override the model's verbose_name_plural
        self.model._meta.verbose_name_plural = model.__name__
    
    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        db = request.GET.get('db', 'default')
        extra_context['current_db'] = db
        extra_context['db_options'] = ['default', 'staging']
        
        # Add database info to page title
        if db == 'default':
            extra_context['title'] = f'🔴 PRODUCTION - {self.model._meta.verbose_name_plural}'
        else:
            extra_context['title'] = f'🟢 STAGING - {self.model._meta.verbose_name_plural}'
            
        return super().changelist_view(request, extra_context)
    
    def get_queryset(self, request):
        db = request.GET.get('db', 'default')
        return super().get_queryset(request).using(db)
    
    def save_model(self, request, obj, form, change):
        db = request.GET.get('db', 'default')
        obj.save(using=db)
    
    def delete_model(self, request, obj):
        db = request.GET.get('db', 'default')
        obj.delete(using=db)

app_models = apps.get_app_config('app').get_models()
for model in app_models:
    try:
        admin.site.register(model, MultiDatabaseModelAdmin)
    except admin.sites.AlreadyRegistered:
        pass  # Skip if already registered