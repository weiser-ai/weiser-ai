import os
import sys
from logging.config import fileConfig
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context
from weiser.drivers.metric_stores.models import MetricRecord
from weiser.loader.config import load_config

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = MetricRecord.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_url():
    """Get database URL from weiser configuration file."""
    # Get weiser config file path from environment variable
    config_file_path = os.getenv("WEISER_CONFIG")

    if config_file_path:
        try:
            weiser_config = load_config(config_file_path, verbose=True)
            # Find the metricstore connection
            for connection in weiser_config.get("connections", []):
                if (
                    connection.get("type") == "metricstore"
                    and connection.get("db_type") == "postgresql"
                ):
                    # Build connection string
                    host = connection.get("host", "localhost")
                    port = connection.get("port", 5432)
                    db_name = connection.get("db_name")
                    user = connection.get("user")
                    password = connection.get("password")

                    if all([db_name, user, password]):
                        return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        except Exception as e:
            print(f"Error loading weiser config: {e}")
            raise e

    # Fallback to environment variable or alembic config
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    return config.get_main_option("sqlalchemy.url")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
