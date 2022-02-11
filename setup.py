from setuptools import find_packages
from setuptools import setup

with open('requirements.txt') as f:
    content = f.readlines()
requirements = [x.strip() for x in content if 'git+' not in x]

setup(name='kiwi_ridesharing',
      version="1.0",
      description="""
      Kiwi is a ride-sharing startup based in New Zealand -
      Their goal is to grow in the competitive market
      """,
      packages=find_packages(),
      install_requires=requirements,
      test_suite='tests',
      # include_package_data: to install data from MANIFEST.in
      include_package_data=True,
      scripts=['scripts/kiwi_ridesharing-run'],
      zip_safe=False)
