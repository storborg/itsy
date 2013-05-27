from setuptools import setup


setup(name="itsy",
      version='0.1',
      description='Web crawling and scraping framework',
      long_description='',
      classifiers=[
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7',
      ],
      url='http://github.com/storborg/itsy',
      author='Scott Torborg',
      author_email='storborg@gmail.com',
      install_requires=[
          # These are for tests.
          'coverage',
          'nose>=1.1',
          'nose-cover3',
      ],
      license='MIT',
      packages=['maitai'],
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)
