from setuptools import find_packages, setup

setup(
    name="my_dagster",
    packages=find_packages(exclude=["my_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-pandas",
        "pandas",
        "sodapy",
        "python-dotenv",
        "pyarrow",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
