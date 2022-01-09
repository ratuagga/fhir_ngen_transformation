import setuptools

setuptools.setup(
    name='dataflow_pipeline',
    version='1.0.0',
    install_requires=['wheel',
                      'apache-beam[gcp]',
                      'google-cloud-storage'],
    packages=setuptools.find_packages(),
)