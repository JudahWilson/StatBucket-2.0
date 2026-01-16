# NBA Basketball Reference Scraper

A comprehensive system for scraping NBA data from basketball-reference.com.

## Technical Design
- Uses 2 Mariadb databases and one sqlite to manage the django metadata.
  - 1 Mariadb instance is for staging data, the other is for production data. They should be identical unless you are testing new features.
  - The database router well never allow you to apply the migrations to the wrong database.
- Uses 2 apps.
  - "app" is the main app that contains the production data models.
  - "staging" is the staging app that contains the staging data models.
