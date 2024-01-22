# Get latest python version
$ asdf latest python
$ asdf latest python 3.10
$ asdf list all python 3.11

# Instlall a python version
$ asdf install python 3.10.13

# Set local python version
$ asdf local python 3.10.13

# Create a new virtualenv
$ virtualenv ~/Virtualenvs/hubspot-mdm

# Create .env files to activate/deactivate virtualenv when in directory
$ echo -e "AUTOENV_ENABLE_LEAVE=1\nsource ~/Virtualenvs/hubspot-mdm/bin/activate" > .env
$ echo deactivate > .env.leave

# Activate virtualenv manually
$ source ~/Virtualenvs/hubspot-mdm/bin/activate


# Install python libraries
$ pip install pandas
$ pip install numpy

# Libraries for data profiling
$ pip install ipywidgets widgetsnbextension pandas-profiling
$ pip install ydata-profiling # data profile reports for CSVs

# Libraries for validation and matching
$ pip install phonenumbers # validate phone numbers
$ pip install validators # validate email, domain, ip_address (ipv4, ipv6)
$ pip install email-validator
$ pip install recordlinkage # matching
$ pip install nameparser

# Libraries for Snowflake and SQL
$ pip install snowflake-connector-python
$ pip install snowflake-snowpark-python

$ pip install pip install 'shandy-sqlfmt[jinjafmt]'


# Create requirements.txt file
$ pip freeze > requirements.txt

# Deactivate virtualenv
$ pyenv deactivate
