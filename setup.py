from setuptools import setup
import versioneer


requires = []

setup(name='papermillhub',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      maintainer='Jim Crist',
      maintainer_email='jiminy.crist@gmail.com',
      license='BSD',
      description='Run papermill on JupyterHub',
      long_description=open('README.rst').read(),
      packages=['papermillhub'],
      install_requires=requires,
      zip_safe=False)
