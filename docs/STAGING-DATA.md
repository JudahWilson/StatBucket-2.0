# Concerns using the same models.py code between Mariadb and SQLite

## Database-Specific Field Types:

```python
# MySQL-specific (would need changes)
class MyModel(models.Model):
    mysql_json = models.JSONField()  # SQLite < 3.38 doesn't support JSON
    big_text = models.TextField(db_collation='utf8mb4_unicode_ci')  # MySQL collation

# Better - database-agnostic
class MyModel(models.Model):
    data = models.JSONField()  # Works on modern SQLite/MySQL
    big_text = models.TextField()  # No database-specific options
```

## Field Size Constraints:

```python
# Might need adjustment
class MyModel(models.Model):
    # MySQL can handle larger values
    big_number = models.BigIntegerField()  # SQLite has different integer handling
    precise_decimal = models.DecimalField(max_digits=65, decimal_places=30)  # MySQL max
```

## Custom db_table Names:

```python
# Usually fine, but watch for reserved words
class MyModel(models.Model):
    class Meta:
        db_table = 'order'  # 'order' might be reserved in some database
```

## Database Functions in Queries:

```python
# Database-specific functions might not work
MyModel.objects.extra(select={'custom': "DATE_FORMAT(created, '%Y-%m')"})  # MySQL only
```
