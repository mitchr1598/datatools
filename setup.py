from distutils.core import setup

# python setup.py sdist

setup(
    name='datatools',
    packages=['datatools'],
    version='1.0.0',
    description='A set of tools for using data',
    author='mitchr1598',
    author_email='mitchr1598@gmail.com',
    url='https://github.com/user/mitchr1598/datatools',  # I explain this later on
    install_requires=[
        'pandas',
        'requests',
    ],
    classifiers=[
        'Programming Language :: Python :: 3.9',
    ],
)
