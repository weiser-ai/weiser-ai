"""
Base class for DuckDB migrations.
"""

from abc import ABC, abstractmethod
from typing import Any
from sqlalchemy import text
from sqlmodel import Session


class BaseMigration(ABC):
    """
    Base class for DuckDB migrations.
    
    Each migration should inherit from this class and implement
    the upgrade() and downgrade() methods.
    """
    
    def __init__(self, version: str, description: str):
        self.version = version
        self.description = description
    
    @abstractmethod
    def upgrade(self, session: Session) -> None:
        """
        Apply the migration (forward).
        
        Args:
            session: SQLModel session for database operations
        """
        pass
    
    @abstractmethod
    def downgrade(self, session: Session) -> None:
        """
        Revert the migration (backward).
        
        Args:
            session: SQLModel session for database operations
        """
        pass
    
    def execute_sql(self, session: Session, sql: str) -> Any:
        """
        Helper method to execute raw SQL safely.
        
        Args:
            session: SQLModel session
            sql: SQL statement to execute
            
        Returns:
            Result of the SQL execution
        """
        return session.exec(text(sql))
    
    def __str__(self) -> str:
        return f"Migration {self.version}: {self.description}"
    
    def __repr__(self) -> str:
        return f"<Migration(version='{self.version}', description='{self.description}')>"