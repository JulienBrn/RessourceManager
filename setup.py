from setuptools import setup, find_packages


setup(
    name='RessourceManager',
    packages=find_packages(where='src'),
    package_data={
        "RessourceManager.package_data": ["*"],
    },
    entry_points={
        'console_scripts': [
            'RessourceManager = RessourceManager:run',
        ]
    },
    version='0.1',
    license='MIT',
    description = 'My package description',
    description_file = "README.md",
    author="Julien Braine",
    author_email='julienbraine@yahoo.fr',
    url='https://github.com/JulienBrn/RessourceManager',
    download_url = 'https://github.com/JulienBrn/RessourceManager.git',
    package_dir={'': 'src'},
    keywords=['python'],
    install_requires=[],
    #['pandas', 'matplotlib', 'PyQt5', "sklearn", "scikit-learn", "scipy", "numpy", "tqdm", "beautifullogger", "statsmodels", "mat73", "psutil"],
    python_requires=">=3.10"
)
