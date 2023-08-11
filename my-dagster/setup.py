from setuptools import find_packages, setup

setup(
    name="my_dagster",
    packages=find_packages(exclude=["my_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "sodapy",
        "python-dotenv"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
