from setuptools import setup, find_packages

setup(
    name="bigdata-ingestion",
    version="1.0.0",
    description="EA1 - Ingestión de datos desde CoinGecko API",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31.0",
        "pandas>=2.0.0",
    ],
    python_requires=">=3.10",
)
