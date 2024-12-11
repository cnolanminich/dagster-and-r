from setuptools import find_packages, setup

setup(
    name="dagster_and_r",
    packages=find_packages(exclude=["dagster_and_r_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "dagster-pipes",
        "dagster-docker",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
