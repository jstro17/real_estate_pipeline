from setuptools import setup, find_packages

setup(
    name="real_estate_analytics",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        line.strip()
        for line in open("requirements.txt")
        if line.strip() and not line.startswith("#")
    ],
    author="Justin Castro",
    description="ETL pipeline for real estate analytics using HUD data",
    python_requires=">=3.8",
)