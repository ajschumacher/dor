from setuptools import setup


description = 'data Diffing, versiOning, meRging'

with open('README.rst') as file:
    long_description = file.read()

url = 'https://github.com/ajschumacher/dor'

setup(name='dor',
      packages=['dor'],
      description=description,
      long_description=long_description,
      license='MIT',
      author='Aaron Schumacher',
      author_email='ajschumacher@gmail.com',
      url=url,
      download_url=url,
      version='0.0.1',
      entry_points={'console_scripts': ['dor = dor.dor:main']},
      classifiers=["Programming Language :: Python",
                   "Programming Language :: Python :: 3.5",
                   "License :: OSI Approved :: MIT License",
                   "Development Status :: 2 - Pre-Alpha"])
