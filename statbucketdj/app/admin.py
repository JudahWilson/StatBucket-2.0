from django.contrib import admin
from django.apps import apps

################################################################################
# Dynamic Model Registration with smart Pluralization
################################################################################

def smart_pluralize(word):
    """Smart pluralization that handles common English rules"""
    word = word.lower()
    
    # Words that don't change (already plural or irregular)
    no_change = ['players', 'coaches', 'games', 'stats']
    if word in no_change:
        return word.title()
    
    # Common patterns
    if word.endswith('s'):
        return word.title()  # Already plural
    elif word.endswith('y'):
        return (word[:-1] + 'ies').title()
    elif word.endswith(('sh', 'ch', 'x', 'z')):
        return (word + 'es').title()
    else:
        return (word + 's').title()

# Get all models from the current app
app_models = apps.get_app_config('app').get_models()
for model in app_models:
    try:
        # Create a dynamic admin class
        admin_class = type(f'{model.__name__}Admin', (admin.ModelAdmin,), {
            'verbose_name_plural': smart_pluralize(model.__name__)
        })
        
        admin.site.register(model, admin_class)
    except admin.sites.AlreadyRegistered:
        pass
