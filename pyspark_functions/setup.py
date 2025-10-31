import setuptools
 
with open("README.md", "r") as fh:
    long_description = fh.read()
 
setuptools.setup(
    name="pyspark-functions",
    version="0.0.4",
    author="Raphael Joebges",
    author_email="r.joebges@enbw.com",
    description="Example for creating and installing a wheel on a Databricks Cluster",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)