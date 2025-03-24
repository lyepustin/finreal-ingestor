from setuptools import setup, find_packages

setup(
    name="finreal-ingestor",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "supabase",
        "python-dotenv",
    ],
) 