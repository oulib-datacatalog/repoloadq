from setuptools import setup, find_packages
setup(name='repoloadq',
      version='0.0.2',
      packages=find_packages(),
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
      ],
      include_package_data=True,
)
