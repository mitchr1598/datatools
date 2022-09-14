from distutils.core import setup

setup(
    name='datatools',
    packages=['datatools'],
    version='0.1.',
    description='A set of tools for using data',
    author='mitchr1598',
    author_email='mitchr1598@gmail.com',
    url='https://github.com/user/mitchr1598',  # I explain this later on
    install_requires=[
        'pandas',
        'requests',
    ],
    classifiers=[
        'Programming Language :: Python :: 3.9',
    ],
)
