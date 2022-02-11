# Data analysis

- Description:
    Kiwi is a fictional ride-sharing startup based in New Zealand -
    Their goal is to grow in the competitive market. This project includes all the
    packages, and steps used to analyze Kiwi's current status and provide recommendations
    on how to best expand their business.

- Data Source:
  Kiwi provided us access to its Data warehouse which contains these three tables:
  - driver_ids
  - ride_ids
  - ride_timestamp

- Type of analysis:
  EDA, Regression, Clustering, NLP

# Startup the project
The initial setup.

Create virtualenv and install the project:
```bash
sudo apt-get install virtualenv python-pip python-dev
deactivate; virtualenv ~/venv ; source ~/venv/bin/activate ;\
    pip install pip -U; pip install -r requirements.txt
```

Unittest test:
```bash
make clean install test
```

Functionnal test with a script:

```bash
cd
mkdir tmp
cd tmp
kiwi_ridesharing-run
```

# Install

Go to `https://github.com/{group}/kiwi_ridesharing` to see the project, manage issues,
setup you ssh public key, ...

Create a python3 virtualenv and activate it:

```bash
sudo apt-get install virtualenv python-pip python-dev
deactivate; virtualenv -ppython3 ~/venv ; source ~/venv/bin/activate
```

Clone the project and install it:

```bash
git clone git@github.com:{group}/kiwi_ridesharing.git
cd kiwi_ridesharing
pip install -r requirements.txt
make clean install test                # install and test
```
Functionnal test with a script:

```bash
cd
mkdir tmp
cd tmp
kiwi_ridesharing-run
```
