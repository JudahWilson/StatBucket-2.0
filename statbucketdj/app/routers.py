class DatabaseRouter:
    """
    Route the auth tables as always the prod db instead of the staging db.
    """
    
    def db_for_read(self, model, **hints):
        """Always read auth tables from default database"""
        if model._meta.app_label == 'auth':
            return 'default'
        return None
    
    def db_for_write(self, model, **hints):
        """Always write auth tables to default database"""
        if model._meta.app_label == 'auth':
            return 'default'
        return None
    
    def allow_relation(self, obj1, obj2, **hints):
        """Allow relations if models are in the same app"""
        db_set = {'default', 'staging'}
        if obj1._state.db in db_set and obj2._state.db in db_set:
            return True
        return None
    
    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """Control which migrations run on which database"""
        
        # Auth tables only migrate to default
        if app_label == 'auth':
            return db == 'default'
        
        # App tables can migrate to both
        if app_label == 'app':
            return True
            
        return None