# pg-deepcompare

*Becuase just comparing the schema is for people that trust their tools.*

## Requirements
- Python 3
- See requirements.txt for specific module requirements

## Setup

- Spin up and enter a virtualenv with `virtualenv <env name>` and `source <env name>/bin/activate`
- Install requirements with `pip install -r requirements.txt`
- copy `task.cfg.sample` to `task.cfg` and fill out the database information
    - Alternatively, if the program is run without a `task.cfg` file present, it will ask for database information for the truth and test databases.
    - Passing database configuration via command line arguments is a planned feature

------------

Copyright 2018-2019 PatientsLikeMe. Distributed under the MIT license. See LICENSE.txt for further details. 
