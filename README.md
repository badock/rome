
# ROME (Relational Object Mapping Extension)

[![Travis-ci Build Status](https://api.travis-ci.org/badock/rome.png?branch=master)](https://travis-ci.org/badock/rome)

## Introduction

ROME (Relational Object Mapping Extension) is an ORM for key/value stores.
In short, it enables to use the SQLAlchemy API with key/value stores.
ROME enables the support of operation such as transactions and joining to key/value stores.

## Installation

    
### Python dependencies

ROME needs some python dependencies, run the following commands in a
shell:

```shell
pip install -r requirements.txt
```

Then install the Rome library with the following command:

```shell
python setup.py install
```


### Running tests

You can execute the following command:

```shell
python execute_tests.py
```

or directly call this python command:

```shell
python -m unittest discover rome.tests "*.py" -v
```

## Usage

### Entity classes

You can directly use SQLAlchemy's entity classes, as in the following example:

```python
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

eng = create_engine('sqlite:///:memory:')

Base = declarative_base()


class Author(Base):
    __tablename__ = "Authors"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Book(Base):
    __tablename__ = "Books"

    id = Column(Integer, primary_key=True)

    title = Column(String)
    price = Column(Integer)
    author_id = Column(Integer, ForeignKey("Authors.id"))

    Author = relationship(Author,
                          backref=orm.backref('books'),
                          foreign_keys=author_id)
```

### Session

To create a session, create instance of the **'rome.core.session.session.Session'** class, as in the following example:

```python
from rome.core.session.session import Session as RomeSession
session = RomeSession()
```

### Queries

Create queries by calling the *query* function of an instance '', as in the following example:

```python
query = session.query(models_module.Author).filter(models_module.Author.id == 1)
results = query.all()
first_result = query.first()
```

### Joining several datasets

Rome enables to perform joining operations (as in classical SQL databases) over data from key/value stores.

You can do a join in the same way as SQLAlchemy does it, as illustrated by the following example:

```python
query = session.query(models_module.Author, models_module.Book).join(models_module.Book)
results = query.all()
```

### Use functions

Rome supports several SQL functions:
- count
- min
- max
- sum

To use them, simply use those provided by SQLAlchemy, as illustrated in the following example:

```python
from sqlalchemy.sql.functions import sum

query = session.query(sum(models_module.Author.id))
result = query.all()
```

### Transactions

Rome supports transactions. Using transactions results in a code that is close to the SQLAlchemy's equivalent, as illustrated in the following example:

```python
session = RomeSession()

with session.begin():
    first_book = session.query(models_module.Book).filter(
        models_module.Book.author_id == None).first()

    session.add(first_book)

    first_available_address.author_id = author_id
```

Each time that the transaction of a session fails, it triggers a **rome.core.session.session.DBDeadlock** exception.

To enable an execution of a python function to retry when a transaction fails, use the **rome.core.session.utils.retry_on_db_deadlock** decoractor, as in the following example:

```python
from rome.core.session.utils import retry_on_db_deadlock


@retry_on_db_deadlock
def book_author_allocate_with_retry(author_id, models_module):
	session = RomeSession()
	
	with session.begin():
	    first_book = session.query(models_module.Book).filter(
	        models_module.Book.author_id == None).first()
	
	    session.add(first_book)
	
	    first_available_address.author_id = author_id
```
