from setuptools import setup, find_packages


setup(
    name="parquet_tools",
    version="0.1.0",
    description='''
    Parquet tools for CLI users.
    ''',
    url="https://github.com/ktrueda/parquet-tools",
    packages=find_packages(),
    install_requires=['pyarrow', 'pandas', 'tabulate'],
    include_package_data=True,
    entry_points="""
  [console_scripts]
  pq = parquet_tools.cli:main
  """,
)
