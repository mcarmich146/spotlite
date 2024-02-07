from setuptools import setup, find_packages

setup(
    name='spotlite',  # Replace with your package's name
    version='0.8.0',  # Replace with your package's version
    author='Mark',  # Replace with your name
    author_email='your.email@example.com',  # Replace with your email
    description='Package to simplify working with Satellogic APIs',  # Provide a short description
    long_description=open('README.md').read(),  # Long description read from the the readme file
    long_description_content_type='text/markdown',  # Long description content type
    url='https://github.com/mcarmich146/spotlite',  # Link to your package's GitHub repo or website
    packages=find_packages(exclude=('tests', 'docs')),  # Automatically find your package
    install_requires=[
        'affine==2.4.0',
        'attrs==23.1.0',
        'branca==0.6.0',
        'cachetools==5.3.2',
        'certifi==2023.7.22',
        'charset-normalizer==3.3.2',
        'click==8.1.7',
        'click-plugins==1.1.1',
        'cligj==0.7.2',
        'contourpy==1.1.1',
        'cycler==0.12.1',
        'fiona==1.9.5',
        'folium==0.14.0',
        'fonttools==4.43.1',
        'geographiclib==2.0',
        'geojson==3.0.1',
        'geopandas==0.14.0',
        'geopy==2.4.0',
        'h11==0.14.0',
        'httplib2==0.22.0',
        'idna==3.4',
        'imageio==2.33.1',
        'jinja2==3.1.3',
        'jsonschema==4.19.2',
        'jsonschema-specifications==2023.7.1',
        'kiwisolver==1.4.5',
        'MarkupSafe==2.1.3',
        'matplotlib==3.8.1',
        'numpy==1.26.1',
        'oauthlib==3.2.2',
        'outcome==1.3.0.post0',
        'packaging==23.2',
        'pandas==2.1.2',
        'pillow==10.2.0',
        'plotly==5.18.0',
        'protobuf==4.25.0',
        'pyasn1==0.5.0',
        'pyasn1-modules==0.3.0',
        'pyparsing==3.1.1',
        'pyproj==3.6.1',
        'PySocks==1.7.1',
        'pystac==1.9.0',
        'pystac-client==0.7.5',
        'python-dateutil==2.8.2',
        'pytz==2023.3.post1',
        'rasterio==1.3.9',
        'referencing==0.30.2',
        'requests==2.31.0',
        'requests-oauthlib==1.3.1',
        'rpds-py==0.10.6',
        'rsa==4.9',
        'schedule==1.2.1',
        'selenium==4.15.1',
        'shapely==2.0.2',
        'six==1.16.0',
        'sniffio==1.3.0',
        'snuggs==1.4.7',
        'sortedcontainers==2.4.0',
        'tenacity==8.2.3',
        'trio==0.22.2',
        'trio-websocket==0.11.1',
        'tzdata==2023.3',
        'uritemplate==4.1.1',
        'urllib3==2.0.7',
        'wsproto==1.2.0',
        'google-api-python-client>=2.114.0',
        'google_auth_oauthlib>=1.2.0'
    ],
    classifiers=[
        # Choose classifiers: https://pypi.org/classifiers/
        'Development Status :: 3 - Alpha',  
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',  # Choose the appropriate license
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
    ],
    python_requires='>=3.11.5',  # Specify the Python versions you support here
    entry_points={
        'console_scripts': [
            # If your package has scripts, put them here, e.g.,
            # 'script_name = your_package.module:function',
        ],
    },
)
