from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()
setup(
    name="parquet_tools",
    version="0.1.2",
    long_description=readme,
    long_description_content_type='text/markdown',
    description='''
    Parquet tools for CLI users.
    ''',
    url="https://github.com/ktrueda/parquet-tools",
    packages=find_packages(),
    license='MIT',
    author='Kentaro Ueda',
    author_email='kentaro.ueda.kentaro@gmail.com',
    keywords='parquet parquet-tools',
    install_requires=['pyarrow', 'pandas', 'tabulate', 'boto3'],
    include_package_data=True,
    entry_points="""
  [console_scripts]
  parquet-tools = parquet_tools.cli:main
  """,
)
