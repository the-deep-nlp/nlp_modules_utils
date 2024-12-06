from setuptools import setup, find_packages

setup(
    name="nlp_modules_utils",
    author="rsh",
    author_email="",
    description="Helper utils functions",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    # Dependencies
    install_requires=[
        "psycopg2-binary>=2.9.6",
        "boto3>=1.26.106",
        "requests>=2.31.0"
    ],
    entry_points={
    },
    version="0.1",
    license="MIT",
    long_description=open("README.md").read(),
)
