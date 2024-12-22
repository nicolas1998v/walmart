"""
Setup file for walmart_pipeline_2 package - Configures Apache Beam pipeline for processing Walmart data
--------------------------------
22-Dec-2024 v01 Nicolas VELEZ. First draft done. All uploaded to GitHub.    
"""


from setuptools import setup, find_packages

setup(
    name="walmart_pipeline",
    version="0.1.0",
    description="Apache Beam pipeline for Walmart data processing",  # Brief description
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.7",  #  minimum Python version
    install_requires=[
        "apache-beam[gcp]>=2.40.0",  #  minimum versions for stability
        "pandas>=1.3.0",
        "pyarrow>=6.0.0"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)