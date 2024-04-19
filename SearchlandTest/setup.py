from setuptools import find_packages, setup

setup(
    name="SearchlandTest",
    packages=find_packages(exclude=["SearchlandTest_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
