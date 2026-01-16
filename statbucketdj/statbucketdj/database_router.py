"""
Database router for directing app and staging models to appropriate databases.
"""

class DatabaseRouter:
    """
    Route database operations for app and staging models to their respective databases.
    """
    
    route_app_labels = {
        'app': 'prod',        # app models go to prod database
        'staging': 'staging'  # staging models go to staging database
    }

    def db_for_read(self, model, **hints):
        """Suggest the database to read from."""
        if model._meta.app_label in self.route_app_labels:
            return self.route_app_labels[model._meta.app_label]
        return None

    def db_for_write(self, model, **hints):
        """Suggest the database to write to."""
        if model._meta.app_label in self.route_app_labels:
            return self.route_app_labels[model._meta.app_label]
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """Allow relations if models are in the same app."""
        db_set = {'prod', 'staging'}
        if obj1._state.db in db_set and obj2._state.db in db_set:
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """Ensure that certain apps' models get created on the right database."""
        if app_label == 'app':
            return db == 'prod'
        elif app_label == 'staging':
            return db == 'staging'
        return None