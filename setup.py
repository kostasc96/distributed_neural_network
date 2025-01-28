import setuptools

src_packages = setuptools.find_packages(where="pcomp_utils")
REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()]

setuptools.setup(
    name="pcomp", #package_name
    version="0.1",
    author="pcomp",
    author_email=None,
    description="Package containining utils for pcomp",
    long_description="This package contains the functions that will be used for pcomp",
    long_description_content_type="text/markdown",
    packages = [f"pcomp.{pkg}" for pkg in src_packages] + ["pcomp"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    include_package_data=True,
    install_requires=REQUIREMENTS,
    package_dir={"pcomp": "pcomp_utils"},  #top-level folder -> pcomp (mapping to pcomp_utils)
    package_data={"pcomp": ["*"]}
)