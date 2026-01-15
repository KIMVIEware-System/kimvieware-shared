from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="kimvieware-shared",
    version="0.1.0",
    author="Flavie Ndemafo",
    author_email="davila.ndemafo@facsciences-uy1.cm",  
    description="Shared libraries for KIMVIEware platform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/KIMVIEware-System/kimvieware-shared",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Testing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.10",
    install_requires=[
        "pika>=1.3.2",
        "pymongo>=4.6.1",
        "redis>=5.0.1",
        "pydantic>=2.5.0",
        "prometheus-client>=0.19.0",
    ],
)
