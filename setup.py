import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sahandler-YOUR-USERNAME-HERE", # Replace with your own username
    version="0.0.1",
    author="Spencer Siu",
    author_email="spencer@topocean.com",
    description="Some python libs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/topocean/python-app-includes",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.6',
)