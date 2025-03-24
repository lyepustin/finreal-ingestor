from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, UUID4
from enum import Enum


class AccountType(str, Enum):
    BANK_ACCOUNT = "BANK_ACCOUNT"
    VIRTUAL_CARD = "VIRTUAL_CARD"


class Bank(BaseModel):
    __tablename__ = 'banks'
    id: int
    name: str
    user_id: UUID4
    
    class Config:
        from_attributes = True


class Account(BaseModel):
    __tablename__ = 'accounts'
    id: int
    bank_id: int
    account_type: AccountType
    account_number: str
    
    class Config:
        from_attributes = True


class Transaction(BaseModel):
    __tablename__ = 'transactions'
    id: int
    uuid: UUID4
    account_id: int
    operation_date: datetime
    value_date: Optional[datetime] = None
    inserted_at: datetime
    description: str
    user_description: Optional[str] = None
    
    class Config:
        from_attributes = True


class TransactionCategory(BaseModel):
    __tablename__ = 'transaction_categories'
    id: int
    transaction_id: int
    category_id: int
    subcategory_id: int
    amount: float
    
    class Config:
        from_attributes = True


class Category(BaseModel):
    __tablename__ = 'categories'
    id: int
    name: str
    user_id: UUID4
    
    class Config:
        from_attributes = True


class SubCategory(BaseModel):
    __tablename__ = 'subcategories'
    id: int
    category_id: int
    name: str
    
    class Config:
        from_attributes = True


class Rule(BaseModel):
    __tablename__ = 'transaction_rules'
    id: int
    pattern: str
    category_id: int
    subcategory_id: int
    user_id: UUID4
    
    class Config:
        from_attributes = True