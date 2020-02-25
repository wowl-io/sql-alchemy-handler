import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sahandler",
    version="0.0.3",
    author="Spencer Siu",
    author_email="spencer@topocean.com",
    description="SQL Alchemy helpers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/topocean/sql-alchemy-handler",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.6',
)
